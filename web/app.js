"use strict";

// 深空标定控制台前端：持续轮询 + 单调渲染，防止过期响应覆盖封存结果。
(() => {
  const STATUS_RANK = {
    AWAITING_FIRST_VOTE: 0,
    COLLECTING: 1,
    SEALED: 2,
  };

  // 已渲染状态的“坐标”：严格单调推进，回退/同版本重复一律丢弃。
  // 即便旧请求晚于新请求返回（过期响应），也无法把 SEALED 拉回 COLLECTING。
  let renderedVersion = -1;
  let renderedRank = -1;
  let watchingBatch = null;
  let pollTimer = null;
  let requestSeq = 0;

  const $ = (id) => document.getElementById(id);
  const escapeHtml = (s) =>
    String(s).replace(/[&<>"']/g,
      (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;",
               '"': "&quot;", "'": "&#39;" }[c]));

  async function postJson(url, body) {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });
    let data = null;
    try {
      data = await res.json();
    } catch (e) {
      data = null;
    }
    return { ok: res.ok, status: res.status, data };
  }

  async function fetchState(batchId) {
    const seq = ++requestSeq;
    let data = null;
    try {
      const res = await fetch("/api/batches/" + encodeURIComponent(batchId),
                              { cache: "no-store" });
      if (res.status === 304) return { seq, state: null, unchanged: true };
      data = await res.json();
      if (!res.ok) throw data;
      return { seq, state: data, unchanged: false };
    } catch (err) {
      return { seq, error: err, state: null };
    }
  }

  // 只接受严格更新的状态；封存后任何更旧状态都不允许覆盖。
  function acceptState(state) {
    if (!state) return false;
    const rank = STATUS_RANK[state.status];
    if (typeof rank !== "number") return false;
    // 已渲染封存态：只接受仍为封存态且版本不回退的状态。
    if (renderedRank === STATUS_RANK.SEALED) {
      if (rank !== STATUS_RANK.SEALED) return false;
      if (state.version < renderedVersion) return false;
      renderedVersion = state.version;
      return true;
    }
    if (rank < renderedRank) return false;
    if (rank === renderedRank && state.version <= renderedVersion) return false;
    renderedRank = rank;
    renderedVersion = state.version;
    return true;
  }

  function resetMonotonic() {
    renderedVersion = -1;
    renderedRank = -1;
  }

  function setFeedback(el, kind, html) {
    el.className = "feedback " + kind;
    el.innerHTML = html;
  }

  function describeError(data, fallback) {
    if (!data) return escapeHtml(fallback);
    let msg = escapeHtml(data.message || fallback);
    if (data.code) msg = "<strong>[" + escapeHtml(data.code) + "]</strong> " + msg;
    if (data.hint) msg += '<br><span style="color:#93a2d0">建议：' +
      escapeHtml(data.hint) + "</span>";
    return msg;
  }

  // ---- 配置批次 -------------------------------------------------------
  $("config-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const f = ev.target;
    const stations = f.stations.value
      .split(/[,，\n\r]+/)
      .map((s) => s.trim())
      .filter(Boolean);
    const body = {
      batch_id: f.batch_id.value.trim(),
      stations,
      threshold: parseInt(f.threshold.value, 10),
    };
    const { ok, data } = await postJson("/api/batches", body);
    const fb = $("vote-feedback");
    if (ok) {
      setFeedback(fb, "ok",
        "批次 <strong>" + escapeHtml(data.batch_id) +
        "</strong> 已创建，等待首个有效投票冻结配置。");
      $("watch-id").value = data.batch_id;
      startWatching(data.batch_id);
    } else {
      setFeedback(fb, "err", describeError(data, "批次创建被拒绝"));
    }
  });

  // ---- 提交投票 -------------------------------------------------------
  $("vote-form").addEventListener("submit", async (ev) => {
    ev.preventDefault();
    const f = ev.target;
    const batchId = f.batch_id.value.trim();
    const body = {
      station: f.station.value.trim(),
      vote_id: f.vote_id.value.trim(),
      summary: f.summary.value,
    };
    const fb = $("vote-feedback");
    setFeedback(fb, "info", "提交中…");
    const { ok, data } = await postJson(
      "/api/batches/" + encodeURIComponent(batchId) + "/votes", body);
    if (ok) {
      if (data.decision === "CONFLICT") {
        setFeedback(fb, "err",
          "冲突票已隔离（原因 <strong>" + escapeHtml(data.reason) +
          "</strong>），该票不参与封存。");
      } else if (data.replayed) {
        setFeedback(fb, "info",
          "同一稳定投票标识重传：仅回放首次结果，<strong>不增票</strong>。");
      } else if (data.sealed_now) {
        setFeedback(fb, "ok", "赞成票已记录，<strong>本票触发阈值，证书已封存</strong>。");
      } else {
        setFeedback(fb, "ok", "赞成票已记录。");
      }
      if (data.state) render(data.state);
      if (!$("watch-id").value) {
        $("watch-id").value = batchId;
        startWatching(batchId);
      }
    } else {
      setFeedback(fb, "err", describeError(data, "投票被拒绝"));
    }
  });

  // ---- 轮询 -----------------------------------------------------------
  $("watch-btn").addEventListener("click", () => {
    const id = $("watch-id").value.trim();
    if (id) startWatching(id);
  });

  async function pollOnce() {
    if (!watchingBatch) return;
    const { seq, state, unchanged, error } = await fetchState(watchingBatch);
    // 只采用最新一次发起的请求结果，旧请求迟到结果直接丢弃。
    if (seq !== requestSeq || watchingBatch === null) return;
    if (error) {
      if (!unchanged) {
        const el = $("state");
        if (el.classList.contains("empty")) {
          el.classList.remove("empty");
          el.innerHTML = '<span style="color:#ff9b9b">载入失败：' +
            describeError(error, "无法获取批次状态") + "</span>";
        }
      }
      return;
    }
    if (state && acceptState(state)) render(state);
  }

  function startWatching(batchId) {
    watchingBatch = batchId;
    resetMonotonic();
    $("poll-indicator").textContent = "● 轮询中 · " + batchId;
    $("poll-indicator").style.color = "#7fe0a8";
    clearInterval(pollTimer);
    pollTimer = setInterval(pollOnce, 1500);
    pollOnce();
  }

  // ---- 渲染 -----------------------------------------------------------
  function render(s) {
    const el = $("state");
    el.classList.remove("empty");
    const stations = s.stations;
    const approveSet = new Set(s.approving_stations);
    const conflictSet = new Set(s.conflict_stations);
    const frozen = s.status !== "AWAITING_FIRST_VOTE";
    const statusBadge = s.status === "SEALED"
      ? '<span class="badge sealed">已封存 SEALED</span>'
      : (s.status === "COLLECTING"
          ? '<span class="badge collecting">收集中 COLLECTING</span>'
          : '<span class="awaiting">等待首次有效投票（配置尚未冻结）</span>');

    const stationChips = stations.map((st) => {
      let cls = "chip";
      if (conflictSet.has(st)) cls += " conflict";
      else if (approveSet.has(st)) cls += "";
      else cls += " dim";
      const tag = conflictSet.has(st) ? "（有冲突）"
        : approveSet.has(st) ? "（赞成）" : "";
      return '<span class="' + cls + '">' + escapeHtml(st) +
        escapeHtml(tag) + "</span>";
    }).join("");

    const conflictRows = s.conflicts.map((v) =>
      "<tr><td>" + escapeHtml(v.station) + "</td><td>" +
      escapeHtml(v.vote_id) + "</td><td>" + escapeHtml(v.reason || "") +
      '</td><td class="mono">' + escapeHtml(v.created_at) + "</td></tr>"
    ).join("");

    let html =
      "<dl class='kv'>" +
      "<dt>批次</dt><dd>" + escapeHtml(s.batch_id) + "</dd>" +
      "<dt>状态</dt><dd>" + statusBadge + " <small style='color:#7d8ab4'>" +
        "版本 v" + s.version + "</small></dd>" +
      "<dt>固定名单 / 阈值</dt><dd>" + escapeHtml(stations.join("、")) +
        " · 阈值 <strong>" + s.threshold + "</strong></dd>" +
      "<dt>赞成票</dt><dd><strong>" + s.approve_count + "</strong> / " +
        s.threshold + "</dd>" +
      (frozen ? "<dt>冻结摘要</dt><dd class='mono'>" +
        escapeHtml(s.frozen_summary) + "</dd>" : "") +
      (s.sealed_at ? "<dt>封存时间</dt><dd class='mono'>" +
        escapeHtml(s.sealed_at) + "</dd>" : "") +
      "</dl>" +
      '<h3 class="section-title">站点</h3><div class="chips">' +
      stationChips + "</div>";

    if (conflictRows) {
      html += '<h3 class="section-title">冲突票（隔离，不计入赞成）</h3>' +
        "<table><thead><tr><th>站点</th><th>vote_id</th><th>原因</th>" +
        "<th>时间</th></tr></thead><tbody>" + conflictRows +
        "</tbody></table>";
    }

    if (s.certificate) {
      const c = s.certificate;
      html +=
        '<div class="cert"><h3>🔒 不可变标定证书</h3>' +
        "<dl class='kv'>" +
        "<dt>证书号</dt><dd class='mono'>" + escapeHtml(c.cert_id) + "</dd>" +
        "<dt>批次</dt><dd>" + escapeHtml(c.batch_id) + "</dd>" +
        "<dt>封存摘要</dt><dd class='mono'>" + escapeHtml(c.summary) + "</dd>" +
        "<dt>赞成站点</dt><dd>" +
          escapeHtml(c.approving_stations.join("、")) + "（" +
          c.approving_stations.length + " 票）</dd>" +
        "<dt>冻结/封存时间</dt><dd class='mono'>" +
          escapeHtml(c.frozen_at) + "<br>" + escapeHtml(c.sealed_at) + "</dd>" +
        "<dt>完整性指纹</dt><dd class='mono'>" + escapeHtml(c.hash) + "</dd>" +
        "</dl></div>";
    }

    el.innerHTML = html;

    // 封存后轮询仍可刷新证书审计信息，但状态不可回退（acceptState 已保证）。
  }

  // 页面刷新后若输入框留有批次则自动恢复轮询。
  const preset = $("watch-id").value.trim();
  if (preset) startWatching(preset);
})();
