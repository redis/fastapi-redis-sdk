// The demo page: three devices, the controls, the wire log and the Redis lens.
//
// Each device is a Pane with its own session (the server keys the cookie by
// ?device=...). A pane follows a run with EventSource, which reconnects by
// itself and sends Last-Event-ID; "Drop VPN" closes it and "Reconnect" opens a
// new one with ?after=<last entry ID>.
"use strict";

const $ = (sel, el = document) => el.querySelector(sel);
const esc = (s) =>
  String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" })[c]);

let health = null;
let focus = "laptop";
const panes = {};

// -- HTTP -------------------------------------------------------------------

const withDevice = (path, device) => `${path}${path.includes("?") ? "&" : "?"}device=${device}`;

const SHOWN_HEADERS = [
  "x-redis-cache", "retry-after", "x-ratelimit-limit", "x-ratelimit-remaining",
  "x-ratelimit-reset", "ratelimit-policy", "ratelimit",
];

async function api(pane, method, path, body, { log = true } = {}) {
  const res = await fetch(withDevice(path, pane.device), {
    method,
    headers: body ? { "content-type": "application/json" } : {},
    body: body ? JSON.stringify(body) : undefined,
  });
  const text = await res.text();
  let data = null;
  try { data = text ? JSON.parse(text) : null; } catch { data = text; }
  if (log) {
    const headers = SHOWN_HEADERS.filter((h) => res.headers.has(h)).map((h) => `${h}: ${res.headers.get(h)}`);
    wire.add({ who: pane.device, method, path, status: `${res.status} ${res.statusText}`, code: res.status,
      blocks: [body ? JSON.stringify(body) : null, [...headers, ...(text ? ["", text] : [])].join("\n") || null] });
  }
  return { status: res.status, headers: res.headers, data };
}

// -- Wire log ---------------------------------------------------------------

const wire = {
  items: [],
  add(item) {
    item.at = new Date().toTimeString().slice(0, 8);
    this.items.unshift(item);
    this.items.length = Math.min(this.items.length, 14);
    this.draw();
    return item;
  },
  draw() {
    $("#wire").innerHTML = this.items.map((x) => {
      const s = `s${String(x.code)[0]}`;
      const icon = { s2: ICONS.good, s4: ICONS.critical, s5: ICONS.critical }[s] ?? "";
      return `
      <div class="exchange ${s}">
        <div class="exchange-head"><span class="ts">${x.at}</span><span class="who-tag">${esc(x.who)}</span><span class="method">${esc(x.method)}</span>
          <span>${esc(x.path)}</span><span class="status ${s}">${icon ? `<span class="status-icon">${icon}</span>` : ""}${esc(x.status)}</span></div>
        ${x.blocks.filter(Boolean).map((b) => `<pre>${esc(b)}</pre>`).join("")}
        ${x.frames && x.frames.length ? `<pre>${esc(x.frames.slice(-6).join("\n\n"))}</pre>` : ""}
      </div>`;
    }).join("");
  },
};

// -- Markdown subset --------------------------------------------------------

const MARK = "⁣";

function inline(s) {
  return esc(s)
    .replace(/`([^`]+)`/g, "<code>$1</code>")
    .replace(/\*\*([^*]+)\*\*/g, "<b>$1</b>")
    .replace(MARK, '<span class="badge pending" title="Text after this point came from the replay">⟲ resumed</span> ');
}

function markdown(text) {
  const out = [];
  let list = null;
  for (const line of text.split("\n")) {
    const ul = line.match(/^- (.*)/);
    const ol = line.match(/^\d+\. (.*)/);
    const kind = ul ? "ul" : ol ? "ol" : null;
    if (list && list !== kind) { out.push(`</${list}>`); list = null; }
    if (kind && !list) { out.push(`<${kind}>`); list = kind; }
    if (kind) out.push(`<li>${inline((ul || ol)[1])}</li>`);
    else if (line.trim()) out.push(`<p>${inline(line)}</p>`);
  }
  if (list) out.push(`</${list}>`);
  return out.join("");
}

const cmpId = (a, b) => {
  const [am, as] = a.split("-").map(Number);
  const [bm, bs] = b.split("-").map(Number);
  return am - bm || as - bs;
};

const newRunId = () =>
  "ru_" + [...crypto.getRandomValues(new Uint8Array(6))].map((b) => b.toString(16).padStart(2, "0")).join("");

const ICONS = { good: "✓", warning: "▲", serious: "◆", critical: "✕" };

// -- A device ---------------------------------------------------------------

class Pane {
  constructor(device, handle, label, { phone = false, composer = true } = {}) {
    Object.assign(this, { device, handle, label, phone, composer, es: null, countdown: null });
    this.resetDevice();
    this.el = document.createElement("div");
    this.el.className = `device ${phone ? "phone" : "laptop"}`;
    this.el.addEventListener("click", () => setFocus(this.device));
    $("#panes").append(this.el);
  }

  // The state of a window opened for the first time.
  resetDevice() {
    this.closeStream();
    clearInterval(this.countdown);
    Object.assign(this, { signedIn: false, streamId: null, conn: "idle", planner: null, quota: null,
      countdown: null, mode: null, spamCodes: null,
      ended: null }); // ended: { code, title, text } after a 401 or a sign-out
    this.resetRun();
  }

  get closed() { return this.el.classList.contains("hidden"); }

  open() {
    this.el.classList.remove("hidden");
    this.render();
    setFocus(this.device);
  }

  // Resets the flow that opened this window: the run this device started
  // ends, the device signs out, and the window opens fresh next time.
  async close() {
    const run = this.composer && this.running ? this.streamId : null;
    this.closeStream();
    this.el.classList.add("hidden");
    if (run) await api(this, "POST", `/api/streams/${run}/cancel`);
    if (this.signedIn) await api(this, "DELETE", "/api/session");
    this.resetDevice();
    this.render();
    if (focus === this.device) {
      const next = Object.values(panes).find((p) => !p.closed);
      if (next) setFocus(next.device);
      else showLens("No data. Every window is closed.");
    }
  }

  resetRun(question = null) {
    Object.assign(this, { question, statuses: [], tools: [], text: "", terminal: null,
      lastId: null, resumeAt: null, banner: null, running: false, received: 0, repeats: 0 });
  }

  // -- sessions --

  async signIn(handle = this.handle) {
    const r = await api(this, "POST", "/api/session", { handle });
    if (r.status !== 200) return this.say("critical", "Sign-in failed", r.data?.detail ?? String(r.status));
    Object.assign(this, { handle, signedIn: true, ended: null, banner: null });
    await this.discover({ log: true });
    this.render();
  }

  signedOut(title, text, code = 401) {
    this.closeStream();
    clearInterval(this.countdown);
    Object.assign(this, { signedIn: false, ended: { code, title, text }, running: false, countdown: null, banner: null });
    this.render();
  }

  async signOutEverywhere() {
    const r = await api(this, "DELETE", "/api/sessions");
    if (r.status === 200) {
      this.signedOut(`Signed out on ${r.data.revoked} devices`, `Every session for ${this.handle} is gone from Redis.`, "✓");
    } else if (r.status === 401) {
      this.signedOut("Not signed in", "Sign in first.");
    }
  }

  // Finds a run in progress: after a reload, or on a second device.  Also how
  // a revoked device learns that its session ended.
  async discover({ log = false } = {}) {
    const r = await api(this, "GET", "/api/triage/runs/active", null, { log });
    if (this.closed) return; // closed while the request was in flight
    if (r.status === 401) {
      if (this.signedIn) this.signedOut("Your session ended", "This device got a 401. Sign in again to follow your runs.");
      return;
    }
    if (r.status !== 200) return;
    const wasSignedIn = this.signedIn;
    this.signedIn = true;
    const id = r.data?.stream_id;
    if (id && id !== this.streamId) {
      this.resetRun("(following the active run)");
      Object.assign(this, { streamId: id, running: true });
      this.follow(null);
    } else if (!wasSignedIn) {
      this.render();
    }
  }

  // -- runs --

  async send(service = this.service(), prompt = this.prompt()) {
    this.spamCodes = null;
    const r = await api(this, "POST", "/api/triage/runs", { run_id: newRunId(), service, prompt });
    this.quota = ["x-ratelimit-remaining", "ratelimit"]
      .filter((h) => r.headers.has(h)).map((h) => `${h}: ${r.headers.get(h)}`);
    if (r.status === 202) {
      this.closeStream();
      this.resetRun(prompt);
      Object.assign(this, { streamId: r.data.stream_id, planner: r.data.planner, running: true });
      this.follow(null);
    } else if (r.status === 429) {
      this.startCountdown(Number(r.headers.get("retry-after") || 60));
    } else if (r.status === 409) {
      this.say("warning", "A run is already active (409)", "Stop it, or wait for it to finish.");
    } else if (r.status === 401) {
      this.signedOut("Your session ended", "Sign in again to start a run.");
    } else {
      this.say("critical", `Could not start (${r.status})`, String(r.data?.detail ?? ""));
    }
    this.render();
    return r.status;
  }

  async spam() {
    const codes = [];
    for (let i = 0; i < 6; i++) codes.push(await this.send());
    this.spamCodes = codes.join(", ");
    if (!this.countdown) this.say("warning", "Six starts in a row", `Answers: ${this.spamCodes}.`);
  }

  // -- reading --

  follow(after) {
    this.closeStream();
    const path = `/api/streams/${this.streamId}/events${after ? `?after=${after}` : ""}`;
    const url = withDevice(path, this.device);
    const es = new EventSource(url);
    const log = wire.add({ who: this.device, method: "SSE", path, status: "open", code: 200,
      blocks: [after ? `after=${after}` : null], frames: [] });
    this.es = es;
    this.conn = "connecting";
    es.onopen = () => { this.conn = "live"; this.render(); };
    // stream.end is the final entry spawn() writes; stream.gap and
    // stream.error are the reader's own events. The SDK's types all start
    // with "stream.".
    for (const type of ["status", "tool", "delta", "stream.end", "stream.gap", "stream.error"]) {
      es.addEventListener(type, (e) => this.onEntry(type, e, log));
    }
    es.addEventListener("error", () => {
      if (es.readyState === EventSource.CLOSED) this.probe(url);
      else { this.conn = "reconnecting"; this.render(); }
    });
    this.render();
  }

  onEntry(type, e, log) {
    let id = e.lastEventId || null;
    // A reader's own event (gap, producer_lost, credential_expired) has no ID; EventSource then repeats the last one.
    if ((type === "stream.error" || type === "stream.gap") && id === this.lastId) id = null;
    if (id && this.lastId && cmpId(id, this.lastId) <= 0) { this.repeats++; return; }
    if (id) this.lastId = id;
    this.received++;
    log.frames.push(`${id ? `id: ${id}\n` : ""}event: ${type}\ndata: ${e.data}`);
    wire.draw();
    const data = JSON.parse(e.data);
    // The server ended the stream at the session's deadline; the run goes on.
    if (type === "stream.error" && data.code === "credential_expired") {
      return this.signedOut("Your session ended", "The stream closed at the session's deadline. Sign in again to follow your runs.");
    }
    // The reader fell behind the trimming: some output was skipped, and the
    // stream goes on.
    if (type === "stream.gap") this.statuses.push(`${data.missing} entries were skipped; the answer is incomplete.`);
    else if (type === "status") this.statuses.push(data.text);
    else if (type === "tool") this.tools.push(data);
    else if (type === "delta") this.text += data.text;
    else this.finish(type, data);
    if (this.banner?.resume) this.banner.text = `${this.received} entries since then, ${this.repeats} repeated.`;
    this.render();
  }

  // The stream has one end marker; how the run ended is in the data.
  finish(type, data) {
    this.closeStream();
    this.running = false;
    if (type === "stream.error") {
      this.terminal = { kind: "critical", text: `The agent stopped unexpectedly (${data.code}). Ask again to start a new run.` };
      return;
    }
    this.terminal = {
      done: { kind: "good", text: "Done" },
      cancelled: { kind: "warning", text: "Stopped. The answer above is incomplete." },
      error: { kind: "critical", text: `The run failed (${data.code}).` },
      producer_lost: { kind: "critical", text: "The agent stopped unexpectedly (producer_lost). Ask again to start a new run." },
    }[data.reason];
  }

  // EventSource hides the status of a refused connection, so ask for it.
  async probe(url) {
    this.closeStream();
    const ctrl = new AbortController();
    let status = 0;
    try {
      status = (await fetch(url, { signal: ctrl.signal })).status;
    } catch { /* offline */ }
    ctrl.abort();
    this.running = false;
    // A reconnect past the final entry: the run is over, nothing more will come.
    if (status === 204) return this.say("good", "The run has ended", "Nothing more to read.");
    if (status === 401) return this.signedOut("Your session ended", "This device got a 401. Sign in again to follow your runs.");
    if (status === 404) return this.say("critical", "Run not found (404)", "It does not exist, or it is not yours.");
    if (status === 410) return this.say("critical", "This answer has expired (410)", "Finished answers stay for 2 minutes. Ask again to start a new run.");
    if (status === 409) return this.say("critical", "Cursor ahead of the stream (409)", "The saved position is past anything this run holds, for example after a Redis failover. Ask again to start a new run.");
    if (status === 422) return this.say("critical", "Invalid cursor (422)", "The saved position is not a valid entry ID.");
    if (status === 503) return this.say("serious", "Too many live readers (503)", "Press Reconnect in a few seconds.");
    this.conn = "down";
    this.say("serious", "Connection lost", "Press Reconnect to resume.");
  }

  drop() {
    if (!this.es) return this.say("warning", "Nothing to drop", "This device is not reading a stream.");
    this.closeStream();
    this.conn = "down";
    this.say("serious", "Connection lost",
      `The run goes on in Redis. The last entry read was ${this.lastId ?? "none"}. Watch XLEN grow in the lens.`);
  }

  reconnect() {
    if (!this.streamId) return this.say("warning", "Nothing to resume", "Start a run first.");
    if (this.es) return this.say("warning", "Already connected", "Drop the VPN first.");
    Object.assign(this, { resumeAt: this.text.length, received: 0, repeats: 0, running: true });
    this.banner = { kind: "good", resume: true, title: `Resumed after ${this.lastId}`, text: "Waiting for entries…" };
    this.follow(this.lastId);
  }

  closeStream() {
    if (this.es) this.es.close();
    this.es = null;
    if (this.conn !== "down") this.conn = "idle";
  }

  // -- UI --

  say(kind, title, text) {
    this.banner = { kind, title, text };
    this.render();
  }

  startCountdown(seconds) {
    clearInterval(this.countdown);
    let left = seconds;
    const tick = () => {
      if (left <= 0) {
        clearInterval(this.countdown);
        this.countdown = null;
        this.banner = null;
      } else {
        const answers = this.spamCodes
          ? ` Answers: ${this.spamCodes}. Busy starts get 409, and each still counts against the quota.`
          : "";
        this.banner = { kind: "warning", title: "Run limit reached (429)", text: `You can start the next run in ${left} s.${answers}` };
      }
      left--;
      this.render();
    };
    this.countdown = setInterval(tick, 1000);
    tick();
  }

  service() { return $("select", this.el)?.value ?? "checkout"; }
  prompt() { return $("input.prompt", this.el)?.value || "Why is checkout slow?"; }

  // The sign-in form and the composer are built once per mode and then only
  // updated, so a re-render on every delta never resets what the user types.
  render() {
    const mode = this.signedIn ? "chat" : "signin";
    if (this.mode !== mode || (mode === "signin" && this.endedShown !== this.ended)) {
      this.mode = mode;
      this.endedShown = this.ended;
      this.el.innerHTML = `<div class="bar-slot"></div><div class="banner-slot"></div>${mode === "chat"
        ? `<div class="chat"></div>${this.composer ? this.composerHtml() : ""}`
        : this.signinHtml()}`;
      const form = $("form.signin", this.el);
      if (form) form.onsubmit = (ev) => { ev.preventDefault(); this.signIn($("input.handle", this.el).value); };
      const send = $("button.send", this.el);
      if (send) send.onclick = () => this.send();
    }
    const conn = {
      live: '<span class="conn live">● live</span>',
      connecting: '<span class="conn idle">◌ connecting</span>',
      reconnecting: '<span class="conn down">◌ reconnecting</span>',
      down: '<span class="conn down">○ offline</span>',
      idle: '<span class="conn idle">○ idle</span>',
    }[this.conn];
    const close = `<button class="light close" title="Close" aria-label="Close ${esc(this.label)}"></button>`;
    $(".bar-slot", this.el).innerHTML = this.phone
      ? `<div class="device-bar"><span class="lights">${close}</span><span>${esc(this.label)}</span>${conn}</div>`
      : `<div class="device-bar"><span class="lights">${close}<span class="light"></span><span class="light"></span></span><span class="url">${esc(location.host)}</span><span>${esc(this.label)}</span>${conn}</div>`;
    $(".light.close", this.el).onclick = (ev) => { ev.stopPropagation(); this.close(); };
    const b = this.banner;
    $(".banner-slot", this.el).innerHTML = b
      ? `<div class="banner ${b.kind}"><span class="status-icon">${ICONS[b.kind]}</span><div><b>${esc(b.title)}</b>${esc(b.text)}</div></div>`
      : "";
    this.el.classList.toggle("focused", focus === this.device);
    if (mode !== "chat") return;
    const chat = $(".chat", this.el);
    chat.innerHTML = this.chatHtml();
    chat.scrollTop = chat.scrollHeight;
    if (!this.composer) return;
    const off = health && !health.features.streams.available;
    $("button.send", this.el).disabled = Boolean(this.running || this.countdown || off);
    $(".quota-chips", this.el).innerHTML = (this.quota ?? [])
      .map((q) => `<span class="${/remaining: 0|r=0/.test(q) ? "zero" : ""}">${esc(q)}</span>`).join("")
      + (this.planner ? `<span>planner: ${esc(this.planner)}</span>` : "");
  }

  signinHtml() {
    const e = this.ended;
    return `<form class="signin">
      ${e ? `<div class="empty"><div><div class="code">${esc(e.code)}</div><h3>${esc(e.title)}</h3><p>${esc(e.text)}</p></div></div>`
          : `<h3>Sign in to Triage</h3><p>Pick a handle. There is no password; this is a demo.</p>`}
      <label>Handle</label><input class="field handle" value="${esc(this.handle)}" pattern="[a-z][a-z0-9_\\-]{0,31}" required>
      <button class="btn primary">Sign in</button></form>`;
  }

  composerHtml() {
    const services = Object.keys(health?.services ?? { checkout: {} });
    return `<div class="composer">
      <div class="composer-row">
        <select class="field">${services.map((s) => `<option>${esc(s)}</option>`).join("")}</select>
        <input class="field grow prompt" value="Why is checkout slow?" maxlength="500">
        <button class="btn primary send">Send</button>
      </div>
      <div class="quota-chips"></div></div>`;
  }

  chatHtml() {
    if (!this.question) return `<p class="hint">Ask about a service. The agent checks its metrics and deploys, then streams a report.</p>`;
    const parts = [`<div class="bubble-user">${esc(this.question)}</div>`];
    for (const s of this.statuses) parts.push(`<div class="status-line">· ${esc(s)}</div>`);
    if (this.tools.length) {
      parts.push(`<div class="tools">${this.tools.map((t) => `
        <div class="tool"><span class="name">${esc(t.name)}</span><span class="ms">${t.ms.toLocaleString("en-US")} ms</span>
        <span class="badge ${t.cache === "HIT" ? "hit" : "miss"}">${t.cache === "HIT" ? "✓ " : ""}${esc(t.cache)}</span><span></span>
        <span class="params">${esc(new URLSearchParams(t.params).toString())}</span></div>`).join("")}</div>`);
    }
    let text = this.text;
    if (this.resumeAt !== null && this.resumeAt < text.length) text = text.slice(0, this.resumeAt) + MARK + text.slice(this.resumeAt);
    let html = markdown(text);
    if (this.running && this.conn === "live") html = html.replace(/(<\/(p|li)>)(?!.*<\/(p|li)>)/s, '<span class="cursor"></span>$1');
    if (html) parts.push(`<div class="answer">${html}</div>`);
    const t = this.terminal;
    if (t) parts.push(`<div class="terminal ${t.kind}"><span class="status-icon">${ICONS[t.kind]}</span>${esc(t.text)}</div>`);
    return parts.join("");
  }
}

// -- Lens -------------------------------------------------------------------

function setFocus(device) {
  focus = device;
  Object.values(panes).forEach((p) => p.el.classList.toggle("focused", p.device === device));
  $("#lens-device").textContent = device;
  refreshLens();
}

function showLens(text) {
  $("#lens-subject").textContent = "—";
  $("#lens").innerHTML = `<p class="obs-empty">${esc(text)}</p>`;
}

// XLEN per stream, one sample per refresh, for the sparkline.
const xlenHistory = {};
const RUN_STATUS = {
  running: ["running", "●"], finished: ["good", ICONS.good],
};

function spark(values) {
  if (values.length < 2) return "";
  const max = Math.max(...values, 1);
  const pts = values.map((v, i) => [(100 * i) / (values.length - 1), 24 - (22 * v) / max + 1]);
  const line = pts.map(([x, y], i) => `${i ? "L" : "M"}${x.toFixed(1)},${y.toFixed(1)}`).join("");
  return `<svg class="spark" viewBox="0 0 100 26" preserveAspectRatio="none" role="img">
    <title>XLEN over the last ${values.length} refreshes: ${values[0]} to ${values.at(-1)}</title>
    <path class="area" d="${line}L100,26L0,26Z"/><path class="line" d="${line}" vector-effect="non-scaling-stroke"/></svg>`;
}

function stat(name, v, history) {
  let kind = "", value, unit = "", sub = "", text = false;
  if (v === null) value = "—";
  else if (name === "TTL") [value, unit] = [v.toLocaleString("en-US"), "s"];
  else if (name === "PTTL") [value, unit] = [v.toLocaleString("en-US"), "ms"];
  else if (typeof v === "number") value = v.toLocaleString("en-US");
  else if (/ s of /.test(v)) {
    const [n, of] = v.split(" s of ");
    [value, unit, sub] = [n, "s", `of ${of} s`];
  }
  else [value, text] = [v, true];
  let icon = "";
  if (name === "status" && RUN_STATUS[v]) [kind, icon] = RUN_STATUS[v];
  if (text && v.length > 10) kind += " wide";
  return `<div class="stat ${kind}"><div class="stat-label">${esc(name)}</div>
    <div class="stat-value${text ? " text" : ""}">${icon ? `<span class="status-icon">${icon}</span>` : ""}${esc(value)}${unit ? `<span class="stat-unit">${unit}</span>` : ""}</div>
    ${sub ? `<div class="stat-sub">${esc(sub)}</div>` : ""}${name === "XLEN" && history ? spark(history) : ""}</div>`;
}

const PREFIX = "redis:fastapi:";
const title = (name) => name.startsWith(PREFIX)
  ? `<span class="prefix">${PREFIX}</span>${esc(name.slice(PREFIX.length))}` : esc(name);

function panel(k, history) {
  return `<section class="panel${k.fields ? "" : " gone"}">
    <div class="panel-head"><span class="panel-title">${title(k.name)}</span><span class="panel-type">${esc(k.type)}</span></div>
    ${k.fields
      ? `<div class="stats">${Object.entries(k.fields).map(([n, v]) => stat(n, v, history)).join("")}</div>`
      : `<p class="obs-empty">No data · key expired</p>`}</section>`;
}

function gauge(m) {
  const share = m.used / m.limit;
  const kind = share >= 1 ? "critical" : share >= 0.6 ? "warning" : "good";
  const icon = kind === "good" ? "" : `<span class="status-icon">${ICONS[kind]}</span>`;
  return `<div class="gauge ${kind}"><div class="gauge-label"><span>${esc(m.label)}</span>
      <span class="v">${icon}${m.used} / ${m.limit}${m.ttl != null ? ` · TTL ${m.ttl.toLocaleString("en-US")} s` : ""}</span></div>
    <div class="gauge-track"><div class="gauge-fill" style="width:${100 * Math.min(share, 1)}%"></div></div></div>`;
}

async function refreshLens() {
  const pane = panes[focus];
  if (!pane || pane.closed) return;
  const path = `/api/lens${pane.streamId ? `?stream_id=${pane.streamId}` : ""}`;
  const r = await api(pane, "GET", path, null, { log: false }).catch(() => null);
  if (!r || pane.closed || focus !== pane.device) return;
  const dot = $("#lens-refresh");
  dot.classList.add("tick");
  setTimeout(() => dot.classList.remove("tick"), 300);
  if (r.status !== 200) {
    return showLens(`No data. ${r.status === 401 ? "Not signed in on this device." : `Lens unavailable (${r.status}).`}`);
  }
  $("#lens-subject").textContent = r.data.subject;
  let history = null;
  const id = r.data.stream_id;
  const xlen = r.data.keys.find((k) => k.type === "Stream")?.fields?.XLEN;
  if (id && xlen != null) {
    history = xlenHistory[id] ??= [];
    history.push(xlen);
    if (history.length > 60) history.shift();
  }
  const keys = r.data.keys.map((k) => panel(k, history));
  if (!id) keys.unshift(`<p class="obs-empty">No run yet. An empty database is a valid start.</p>`);
  $("#lens").innerHTML = keys.join("") + `<section class="panel">
    <div class="panel-head"><span class="panel-title">${title(`${PREFIX}ratelimit:…`)}</span><span class="panel-type">counters</span></div>
    <div class="gauges">${r.data.meters.map(gauge).join("")}</div></section>`;
}

// -- Controls ---------------------------------------------------------------

const laptop = () => panes.laptop;

// A laptop control on a closed laptop only opens it, at the sign-in form.
const onLaptop = (fn) => () => (laptop().closed ? laptop().open() : fn(laptop()));

async function show(device) {
  const pane = panes[device];
  pane.open();
  if (!pane.signedIn) await pane.signIn();
  return pane;
}

const CONTROLS = [
  { step: "1·2", label: "Send", run: onLaptop((p) => p.send()) },
  { step: "3", label: "Drop VPN", run: onLaptop((p) => p.drop()) },
  { step: "3", label: "Reconnect", run: onLaptop((p) => p.reconnect()) },
  { step: "4", label: "Open on phone", run: async () => {
    panes.phone.handle = laptop().handle;
    const phone = await show("phone");
    if (!phone.streamId) phone.say("warning", "No active run", "Start one on the laptop; the phone finds it within a few seconds.");
  } },
  { step: "5", label: "Be Sam", run: async () => (await show("sam")).send(laptop().service(), laptop().prompt()) },
  { step: "6", label: "Ask again ×6", run: onLaptop((p) => p.spam()) },
  { step: "7", label: "Sign out everywhere", run: onLaptop((p) => p.signOutEverywhere()) },
];

function renderControls() {
  $("#controls").innerHTML = CONTROLS.map((c, i) => `
    <button class="control" data-i="${i}">
      ${c.step ? `<span class="step">${c.step}</span>` : ""}${esc(c.label)}</button>`).join("");
  document.querySelectorAll(".control").forEach((b) => {
    b.onclick = () => CONTROLS[Number(b.dataset.i)].run();
  });
}

// -- Start ------------------------------------------------------------------

async function boot() {
  panes.laptop = new Pane("laptop", "alex", "Alex's laptop");
  panes.phone = new Pane("phone", "alex", "Alex's phone", { phone: true, composer: false });
  panes.sam = new Pane("sam", "sam", "Sam's laptop");
  panes.phone.el.classList.add("hidden");
  panes.sam.el.classList.add("hidden");

  health = await (await fetch("/api/health")).json();
  $("#planner-chip").textContent = `planner: ${health.planner}`;
  const firing = Object.entries(health.services).find(([, s]) => s.severity === "critical");
  if (firing) {
    const [name, s] = firing;
    const chip = $("#alert-chip");
    chip.innerHTML = `<span class="status-icon" aria-hidden="true">▲</span><span><b>Critical</b> · ${esc(name)} ${esc(s.alert)} · firing since ${esc(s.since)}</span>`;
    chip.classList.remove("hidden");
  }
  renderControls();
  Object.values(panes).forEach((p) => p.render());
  if (!health.features.streams.available) {
    laptop().say("critical", "Chat is off on this server", health.features.streams.reason);
  }
  setFocus("laptop");
  await laptop().discover({ log: true });

  // Each signed-in device checks its session every few seconds: a revoked
  // phone learns of it here, and a second device finds the active run.
  setInterval(() => Object.values(panes).forEach((p) => {
    if (p.signedIn && !p.closed) p.discover();
  }), 4000);
  setInterval(refreshLens, 2000);
}

boot();
