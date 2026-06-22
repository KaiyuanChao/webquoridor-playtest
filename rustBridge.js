(function () {
  "use strict";

  const DEFAULT_BASE_URL = "http://127.0.0.1:8787";

  function normalizeBaseUrl(baseUrl) {
    return String(baseUrl || DEFAULT_BASE_URL).trim().replace(/\/+$/, "");
  }

  async function request(path, payload = null, options = {}) {
    const controller = new AbortController();
    const timeoutMs = Number(options.timeoutMs || 2200);
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const init = { signal: controller.signal };
      if (payload !== null) {
        init.method = "POST";
        init.headers = { "Content-Type": "application/json" };
        init.body = JSON.stringify(payload);
      }
      const res = await fetch(`${normalizeBaseUrl(options.baseUrl)}${path}`, init);
      return await res.json();
    } finally {
      clearTimeout(timer);
    }
  }

  function health(options = {}) {
    return request("/health", null, { ...options, timeoutMs: options.timeoutMs || 1500 });
  }

  function legal(state, options = {}) {
    return request("/legal", { state }, options);
  }

  function apply(state, move, options = {}) {
    return request("/apply", { state, move }, options);
  }

  function aiMove(state, opts = {}) {
    const payload = { state };
    if (opts.rollouts != null) payload.rollouts = opts.rollouts;
    else if (opts.timeMs != null) payload.timeMs = opts.timeMs;
    else payload.level = opts.level || "strong";
    if (opts.threads != null) payload.threads = opts.threads;
    if (opts.seed != null) payload.seed = opts.seed;
    if (opts.policy != null) payload.policy = !!opts.policy;
    return request("/ai/move", payload, { ...opts, timeoutMs: opts.timeoutMs || 8000 });
  }

  function live(snapshot, options = {}) {
    return request("/live", snapshot, options);
  }

  async function selfplay(opts = {}, onEvent = () => {}) {
    const controller = new AbortController();
    const timeoutMs = Number(opts.timeoutMs || 0);
    const timer = timeoutMs > 0 ? setTimeout(() => controller.abort(), timeoutMs) : 0;
    let sawHeader = false;
    let buffer = "";
    try {
      const res = await fetch(`${normalizeBaseUrl(opts.baseUrl)}/selfplay/start`, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(opts.request || opts),
        signal: controller.signal,
      });
      if (!res.body) throw new Error("selfplay stream missing body");
      const reader = res.body.getReader();
      const decoder = new TextDecoder();
      const consumeLine = line => {
        const trimmed = line.trim();
        if (!trimmed) return;
        const event = JSON.parse(trimmed);
        if (!sawHeader) {
          if (event.type !== "stream_start" || event.protocol !== 1) {
            throw new Error("selfplay stream missing protocol header");
          }
          sawHeader = true;
        }
        onEvent(event);
      };
      for (;;) {
        const { value, done } = await reader.read();
        if (done) break;
        buffer += decoder.decode(value, { stream: true });
        const lines = buffer.split("\n");
        buffer = lines.pop() || "";
        for (const line of lines) consumeLine(line);
      }
      buffer += decoder.decode();
      if (buffer) consumeLine(buffer);
      if (!sawHeader) throw new Error("selfplay stream missing protocol header");
      return true;
    } finally {
      if (timer) clearTimeout(timer);
    }
  }

  window.RustBridge = {
    DEFAULT_BASE_URL,
    normalizeBaseUrl,
    request,
    health,
    legal,
    apply,
    aiMove,
    live,
    selfplay,
  };
})();
