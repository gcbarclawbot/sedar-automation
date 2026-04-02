// MM Company Onboarding Dashboard

let currentSymbol = '';
let currentData   = null;
let pollTimer     = null;
let logExpanded   = false;

// ---------------------------------------------------------------------------
// Init
// ---------------------------------------------------------------------------
document.addEventListener('DOMContentLoaded', () => {
  const input = document.getElementById('tickerIn');
  const params = new URLSearchParams(window.location.search);
  const sym = params.get('ticker') || localStorage.getItem('lastTicker') || '';
  if (sym) { input.value = sym.toUpperCase(); loadCompany(sym.toUpperCase()); }
  input.addEventListener('keydown', e => { if (e.key === 'Enter') loadCompany(input.value.trim().toUpperCase()); });
  input.addEventListener('blur',    () => { const v = input.value.trim().toUpperCase(); if (v && v !== currentSymbol) loadCompany(v); });

  // Poll global running state every 3s
  checkGlobalRunning();
  setInterval(checkGlobalRunning, 3000);

  // Close modal on backdrop click
  document.getElementById('newsModal').addEventListener('click', function(e) {
    if (e.target === this) closeNewsModal();
  });
});

// ---------------------------------------------------------------------------
// Load company
// ---------------------------------------------------------------------------
async function loadCompany(symbol) {
  if (!symbol) return;
  currentSymbol = symbol;
  _expandedLists.clear();
  localStorage.setItem('lastTicker', symbol);
  const url = new URL(window.location);
  url.searchParams.set('ticker', symbol);
  window.history.replaceState({}, '', url);

  document.getElementById('coName').textContent = symbol;
  document.getElementById('coMeta').textContent = 'Loading…';
  document.getElementById('mainLayout').style.display = 'none';
  document.getElementById('splash').style.display = 'none';

  try {
    const res = await fetch(`/api/company/${symbol}`);
    if (res.status === 404) {
      // Not onboarded - check if it exists in universe as a Miner
      const uRes = await fetch(`/api/universe/${symbol}`);
      if (uRes.ok) {
        const u = await uRes.json();
        renderUniversePreview(u);
      } else {
        document.getElementById('coName').textContent = symbol;
        document.getElementById('coMeta').textContent = '';
        document.getElementById('splash').style.display = 'block';
        document.getElementById('runBtn').disabled = true;
        document.getElementById('resetBtn').disabled = true;
      }
      return;
    }
    const data = await res.json();
    currentData = data;
    renderAll(data);
    if (data.is_running) startPolling();
  } catch(e) {
    document.getElementById('coMeta').textContent = 'Error loading';
    console.error('loadCompany error:', e);
  }
}

// ---------------------------------------------------------------------------
// Universe preview (not yet onboarded, but exists in universe as Miner)
// ---------------------------------------------------------------------------
function renderUniversePreview(u) {
  const exchangeBadge = u.exchange ? `<span class="badge" style="background:rgba(99,102,241,.15);color:var(--accent);font-size:10px">${esc(u.exchange)}: ${esc(u.symbol)}</span>` : '';
  const sedarBadge    = u.sedar_party ? `<span class="badge" style="background:rgba(100,116,139,.12);color:var(--muted);font-size:10px">SEDAR #${esc(u.sedar_party)}</span>` : '';
  const notOnboarded  = `<span class="badge" style="background:rgba(245,158,11,.15);color:var(--amber);font-size:10px">Not onboarded</span>`;
  document.getElementById('coName').innerHTML = `${esc(u.name)} ${exchangeBadge} ${sedarBadge} ${notOnboarded}`;
  const comm = u.commodity ? ` · ${esc(u.commodity)}` : '';
  document.getElementById('coMeta').innerHTML = `<span style="color:var(--muted)">${esc(u.company_type)}${comm} — click Onboard to build the R&amp;R timeline</span>`;
  // Show Onboard button (green)
  const btn = document.getElementById('runBtn');
  btn.textContent = '▶ Onboard';
  btn.classList.add('onboard');
  btn.disabled = false;
  document.getElementById('resetBtn').disabled = true;
  document.getElementById('mainLayout').style.display = 'none';
  document.getElementById('splash').style.display = 'none';
}

// ---------------------------------------------------------------------------
// Render everything
// ---------------------------------------------------------------------------
function renderAll(data) {
  const { symbol, exchange, sedar_party, state, filings_by_category, prev_last_run_date, is_running } = data;
  const cat = filings_by_category || {};
  const allF = Object.values(cat).flat();

  // Header
  const coName = allF[0]?.issuer || symbol;
  const exchangeBadge = exchange ? `<span class="badge" style="background:rgba(99,102,241,.15);color:var(--accent);font-size:10px">${esc(exchange)}: ${esc(symbol)}</span>` : '';
  const sedarBadge    = sedar_party ? `<span class="badge" style="background:rgba(100,116,139,.12);color:var(--muted);font-size:10px">SEDAR #${esc(sedar_party)}</span>` : '';
  document.getElementById('coName').innerHTML = `${esc(coName)} ${exchangeBadge} ${sedarBadge}`;

  const aifDate = state.aif_filing_date || '—';
  const asAt    = state.as_at_date || '—';
  const lastRun = state.last_run_date || '—';
  const mode    = state.run_mode || '';
  document.getElementById('coMeta').innerHTML =
    `Last AIF: <strong>${fmtDate(aifDate)}</strong> &nbsp;(as at <strong>${fmtDate(asAt)}</strong>)&nbsp; · &nbsp;`
    + `Last run: ${fmtDate(lastRun)} <span class="badge ${mode==='UPDATE'?'b-upd':'b-full'}">${mode}</span>`;

  const runBtn = document.getElementById('runBtn');
  runBtn.disabled = is_running;
  runBtn.classList.remove('onboard');
  if (!runBtn.querySelector('.spin')) runBtn.textContent = '▶ Update';
  document.getElementById('resetBtn').disabled = is_running;
  document.getElementById('mainLayout').style.display = 'grid';
  document.getElementById('splash').style.display = 'none';

  // Left column docs
  renderAIF(cat['AIF'] || [], state);
  renderDocList('mdaBody', 'cntMda', cat['MD&A'] || [], prev_last_run_date,
    { dedupeAmended: true, maxShow: 2 });

  // NI43-101: only actual technical reports, label with matched project from news releases
  const allNews = cat['NewsRelease'] || [];
  const techReports = (cat['NI43-101']||[]).filter(f => {
    const dt = (f.doc_type||'').toUpperCase();
    return dt.includes('TECHNICAL_REPORT') && !dt.includes('CONSENT') && !dt.includes('CERTIFICATE');
  });
  renderDocList('techBody', 'cntTech', techReports, prev_last_run_date, {
    cardId: 'cardTech',
    labelFn: f => {
      // Find the news release filed 0-3 days AFTER this NI43-101 with llm_project
      // Use CLOSEST match (minimum days gap) to avoid cross-matching when two NI43-101s are close
      const tDate = new Date(f.filing_date.slice(0,10));
      let matched = null, minDiff = 999;
      for (const n of allNews) {
        if (!n.llm_project) continue;
        const nDate = new Date(n.filing_date.slice(0,10));
        const diff = (nDate - tDate) / 86400000;
        if (diff >= 0 && diff <= 3 && diff < minDiff) {
          minDiff = diff;
          matched = n;
        }
      }
      const project = matched?.llm_project || '';
      return project
        ? `NI 43-101 <span style="background:rgba(239,68,68,.15);color:#f87171;font-size:10px;font-weight:600;padding:1px 6px;border-radius:3px;white-space:nowrap;">${esc(project)}</span>`
        : 'NI 43-101';
    },
    rawLabel: true
  });

  // Material Changes: label uses mat_summary if available (from LLM), else generic
  renderDocList('matBody', 'cntMat', cat['MaterialChange'] || [], prev_last_run_date,
    { cardId: 'cardMat',
      labelFn: f => f.mat_summary ? `${f.mat_summary}` : 'Material Change'
    });

  const others = [...(cat['Prospectus']||[]), ...(cat['Acquisition']||[])];
  renderDocList('otherBody', 'cntOther', others, prev_last_run_date,
    { cardId: 'cardOther' });

  // Right: news feed
  renderNews(cat['NewsRelease'] || [], state, prev_last_run_date, cat['NI43-101'] || [], cat['AIF'] || []);
}

// ---------------------------------------------------------------------------
// AIF
// ---------------------------------------------------------------------------
function renderAIF(aifs, state) {
  const el = document.getElementById('aifBody');
  const filed = state.aif_filing_date || '—';
  const asAt  = state.as_at_date || '—';

  // Sort AIFs newest first, prefer amended versions for same filing date
  const sorted = [...aifs].sort((a,b) => b.filing_date.localeCompare(a.filing_date));
  const deduped = dedupeAmended(sorted);
  const baselineAif = deduped.find(f => f.aif_filed) || deduped[0];
  const subsequentAifs = deduped.filter(f => f !== baselineAif);
  const pdfHref = bestPdfLink(baselineAif);

  let html = `<div class="aif-inner">
    <div style="font-size:13px;font-weight:700;color:#cbd5e1;margin-bottom:2px;">As at ${fmtDate(asAt)}</div>
    <div style="font-size:11px;color:var(--muted);">Filed ${fmtDate(filed)}</div>
    ${pdfHref ? `<a class="aif-link" href="${pdfHref}" target="_blank" style="margin-top:5px;display:inline-block;">📄 Open AIF ↗</a>` : ''}
  </div>`;

  // Subsequent AIFs (filed after baseline during tracking period)
  if (subsequentAifs.length) {
    html += subsequentAifs.map(f => {
      const h = bestPdfLink(f);
      return `<div class="doc-row" style="margin-top:4px;border-top:1px solid var(--border);padding-top:4px;">
        <span class="doc-date">${fmtDate(f.filing_date.slice(0,10))}</span>
        <span class="doc-name"><span class="doc-type">Updated AIF</span></span>
        ${h ? `<a class="doc-link" href="${h}" target="_blank">PDF ↗</a>` : ''}
      </div>`;
    }).join('');
  }

  el.innerHTML = html;
}

// ---------------------------------------------------------------------------
// Deduplicate amended filings: if an amended version exists for same date,
// keep only the amended one
// ---------------------------------------------------------------------------
function dedupeAmended(sorted) {
  // Group by filing_date
  const byDate = {};
  for (const f of sorted) {
    const d = f.filing_date.slice(0,10);
    if (!byDate[d]) byDate[d] = [];
    byDate[d].push(f);
  }
  const result = [];
  for (const d of Object.keys(byDate).sort().reverse()) {
    const group = byDate[d];
    // Prefer amended/restated versions
    const amended = group.find(f => (f.doc_type||'').toUpperCase().includes('AMENDED') || (f.doc_type||'').toUpperCase().includes('RESTAT'));
    result.push(amended || group[0]);
  }
  return result;
}

// ---------------------------------------------------------------------------
// Clean doc type label
// ---------------------------------------------------------------------------
function cleanDocType(doc_type) {
  const dt = (doc_type || '').toUpperCase().replace(/_EN$/, '').replace(/_/g, ' ');
  if (dt.includes('INTERIM MDA') || dt.includes('INTERIM MD')) return 'Interim MD&A';
  if (dt.includes('MDA') || dt.includes('MD A')) return 'Annual MD&A';
  if (dt.includes('TECHNICAL REPORT')) return 'Technical Report';
  if (dt.includes('MATERIAL CHANGE')) return 'Material Change';
  if (dt.includes('SHORT FORM PROSPECTUS') || dt.includes('PROSPECTUS NON PRICING') || dt.includes('PROSPECTUS SUPPLEMENT')) return 'Prospectus';
  if (dt.includes('PRELIMINARY') && dt.includes('PROSPECTUS')) return 'Preliminary Prospectus';
  if (dt.includes('BUSINESS ACQUISITION')) return 'Business Acquisition';
  if (dt.includes('ANNUAL INFORMATION FORM')) return 'AIF';
  return dt.toLowerCase().replace(/\b\w/g, c => c.toUpperCase());
}

// ---------------------------------------------------------------------------
// Doc list (MD&A, NI43-101, etc.)
// ---------------------------------------------------------------------------
function renderDocList(bodyId, cntId, filings, prevRun, opts = {}) {
  const el      = document.getElementById(bodyId);
  const cnt     = document.getElementById(cntId);
  const cardId  = opts.cardId;
  const maxShow = opts.maxShow || 999;

  // Hide entire card if empty
  if (!filings.length) {
    el.innerHTML = '';
    cnt.textContent = '0';
    cnt.style.background = 'rgba(100,116,139,.15)';
    cnt.style.color = 'var(--muted)';
    if (cardId) document.getElementById(cardId).style.display = 'none';
    return;
  }

  if (cardId) document.getElementById(cardId).style.display = '';
  cnt.style.background = '';
  cnt.style.color = '';

  let sorted = [...filings].sort((a,b) => b.filing_date.localeCompare(a.filing_date));

  // For MD&A: deduplicate amended versions (keep only amended if same date)
  if (opts.dedupeAmended) sorted = dedupeAmended(sorted);

  cnt.textContent = sorted.length;

  const effectiveMax = _expandedLists.has(bodyId) ? 999 : maxShow;
  const shown = sorted.slice(0, effectiveMax);
  const hidden = sorted.length - shown.length;

  el.innerHTML = shown.map(f => {
    const isNew   = prevRun && f.filing_date > prevRun;
    const label   = opts.labelFn ? opts.labelFn(f) : cleanDocType(f.doc_type);
    const synopsis = (f.synopsis || '').trim();
    // For NI43-101s: show synopsis (often contains project name); skip generic boilerplate
    const showSynopsis = synopsis && !/^(annual information form|interim mda|mda|material change report)/i.test(synopsis);
    let dateFmt = f.filing_date.slice(0,10);
    try { const d = new Date(dateFmt); dateFmt = d.toLocaleDateString('en-GB',{day:'numeric',month:'short',year:'numeric'}); } catch(e) {}
    const pdfHref = bestPdfLink(f);
    return `<div class="doc-row${isNew?' is-new':''}">
      <span class="doc-date" style="font-weight:600;color:#cbd5e1;">${dateFmt}${isNew?'<br><span class="badge b-new">NEW</span>':''}</span>
      <span class="doc-name">
        <span class="doc-type">${opts.rawLabel ? label : esc(label)}</span>
        ${showSynopsis ? `<br><span style="color:var(--text);font-size:11px;">${esc(synopsis.slice(0,70))}</span>` : ''}
      </span>
      ${pdfHref ? `<a class="doc-link" href="${pdfHref}" target="_blank">PDF ↗</a>` : ''}
    </div>`;
  }).join('') + (hidden > 0 ? `<div style="font-size:11px;color:var(--accent);padding:4px 8px;cursor:pointer;user-select:none;" onclick="expandDocList('${bodyId}')">+ ${hidden} more</div>` : '');
}

// ---------------------------------------------------------------------------
// News feed - chronological descending, AIF milestone at bottom
// ---------------------------------------------------------------------------
function renderNews(news, state, prevRun, ni43101Rows, aifRows) {
  const el      = document.getElementById('newsFeed');
  const feedSub = document.getElementById('feedSub');
  const counts  = document.getElementById('flagCounts');

  const aifFiledDate = state.aif_filing_date || '';
  const asAtDate     = state.as_at_date || aifFiledDate;
  const aifYear      = asAtDate ? asAtDate.slice(0,4) : '';
  feedSub.textContent = `${news.length} releases since AIF as at ${fmtDate(asAtDate)}`;

  const changed = news.filter(f => f.llm_flag === 'CHANGED');
  const none    = news.filter(f => !f.llm_flag || f.llm_flag === 'NONE' || f.llm_flag === 'POSSIBLE');

  counts.innerHTML = `
    <span class="badge" style="background:rgba(239,68,68,.2);color:var(--red)">🔴 ${changed.length} R&R Changed</span>
    <span class="badge" style="background:rgba(100,116,139,.15);color:var(--muted)">⚪ ${none.length} No change</span>`;

  if (!news.length) { el.innerHTML = '<div class="empty-msg" style="color:var(--muted);padding:20px">No news releases</div>'; return; }

  // Sort ALL news newest first for unified timeline
  // Include any AIFs filed AFTER the baseline AIF as inline milestone sentinels
  const baselineAifDate = aifFiledDate || '';
  const inlineAifs = (aifRows || [])
    .filter(a => (a.doc_type||'').toUpperCase().includes('ANNUAL_INFORMATION_FORM')
              && a.filing_date.slice(0,10) > baselineAifDate.slice(0,10))
    .map(a => ({...a, _sentinel: 'AIF'}));
  const allSorted = [...news, ...inlineAifs].sort((a,b) => b.filing_date.localeCompare(a.filing_date));

  // CHANGED visible, NONE collapsed (but still in correct position in timeline)
  const changedIds = new Set(changed.map(f => f.filing_date + f.doc_type));
  let html = '';
  let noneBuffer = [];  // collect consecutive NONE items

  function flushNone() {
    if (!noneBuffer.length) return;
    const items = noneBuffer.map((f,i) => newsItem(f, 'none-' + f.filing_date + i, prevRun)).join('');
    html += `<div class="none-section-hdr" onclick="toggleNone(this)" style="margin:2px 0">
      <span>▶</span><span style="font-size:10px">⚪ ${noneBuffer.length} releases — no resource change</span>
    </div><div class="none-section-body">${items}</div>`;
    noneBuffer = [];
  }

  let lastYear = null;

  allSorted.forEach((f, i) => {
    const thisYear = f.filing_date.slice(0,4);
    // Insert year marker when year changes
    if (thisYear !== lastYear) {
      flushNone();
      const currentYear = new Date().getFullYear().toString();
      // Suppress current year (implied) and AIF year only if no news releases fall in that year
      const hasReleasesInAifYear = news.some(n => n.filing_date.slice(0,4) === aifYear);
      const suppress = thisYear === currentYear || (thisYear === aifYear && !hasReleasesInAifYear);
      if (!suppress) {
        html += `<div style="display:flex;align-items:center;gap:8px;margin:8px 0 4px;user-select:none;">
          <span style="font-size:11px;font-weight:700;color:var(--muted);padding-right:8px;">${thisYear}</span>
          <div style="flex:1;height:1px;background:var(--border)"></div>
        </div>`;
      }
      lastYear = thisYear;
    }
    // Override: if a new technical report was filed 0-3 days before the news release, it IS a resource change
    // regardless of LLM flag (covers same-day and Fri→Mon next-trading-day patterns)
    const sameDayTechReport = (ni43101Rows || [])
      .some(t => {
        const dt = (t.doc_type||'').toUpperCase();
        if (!dt.includes('TECHNICAL_REPORT')) return false;
        const tDate = new Date(t.filing_date.slice(0,10));
        const fDate = new Date(f.filing_date.slice(0,10));
        const diffDays = (fDate - tDate) / 86400000;
        return diffDays >= 0 && diffDays <= 3;
      });
    const isChanged = f.llm_flag === 'CHANGED' || sameDayTechReport;
    if (isChanged) {
      flushNone();
      html += newsItem(f, 'c' + i, prevRun);
    } else {
      noneBuffer.push(f);
    }
  });
  flushNone();

  // AIF milestone at bottom - clickable
  const aifPdfLink = (() => {
    const aifs = currentData?.filings_by_category?.['AIF'] || [];
    return bestPdfLink(aifs[0]) || '';
  })();
  // AIF year marker suppressed - the AIF baseline block itself is the visual anchor
  html += `<div style="display:flex;align-items:flex-start;gap:8px;padding:10px 0 6px;margin-top:4px;border-top:2px solid var(--accent);">
    <div style="width:88px;flex-shrink:0;text-align:right;padding-right:10px;">
      <div style="font-size:10px;font-weight:700;color:var(--accent)">AS AT</div>
      <div style="font-size:10px;color:var(--accent)">${fmtDate(asAtDate)}</div>
    </div>
    <span style="font-size:16px;flex-shrink:0">📋</span>
    <div style="flex:1;">
      <div style="font-size:12px;font-weight:700;color:var(--accent)">AIF Resource Baseline</div>
      <div style="font-size:11px;color:var(--muted)">Filed ${fmtDate(aifFiledDate)} — snapshot of R&R as at ${fmtDate(asAtDate)}</div>
      <div style="margin-top:4px;">
        ${aifPdfLink ? `<a href="${aifPdfLink}" target="_blank" style="font-size:11px;color:var(--accent);text-decoration:none;">📄 Open AIF ↗</a>` : ''}
      </div>
    </div>
  </div>`;

  el.innerHTML = html;
}

function newsItem(f, idx, prevRun) {
  const isNew   = prevRun && f.filing_date > prevRun;
  const flag    = f.llm_flag || 'NONE';
  // Structural override: NI43-101 filed 0-3 days before = resource change (covers same-day + Fri→Mon)
  const sameDayTech = (currentData?.filings_by_category?.['NI43-101'] || [])
    .some(t => {
      const dt = (t.doc_type||'').toUpperCase();
      if (!dt.includes('TECHNICAL_REPORT')) return false;
      const tDate = new Date(t.filing_date.slice(0,10));
      const fDate = new Date(f.filing_date.slice(0,10));
      const diffDays = (fDate - tDate) / 86400000;
      return diffDays >= 0 && diffDays <= 3;
    });
  const isChanged = flag === 'CHANGED' || sameDayTech;
  const displaySummary = f.llm_summary || '';
  const displayProject = f.llm_project || '';
  const icon    = isChanged ? '🔴' : '⚪';
  const cls     = isChanged ? 'changed' : 'none-item';
  // Filter out generic SEDAR+ document names that aren't meaningful headlines
  const rawSynopsis = (f.synopsis || '');
  const synopsisIsGeneric = /^news release/i.test(rawSynopsis.trim()) || rawSynopsis.trim().endsWith('.pdf');
  const cleanSynopsis = synopsisIsGeneric ? '' : rawSynopsis;
  // Strip redundant "No resource change;" / "No change;" prefix from LLM summaries
  const cleanSummary = (displaySummary || cleanSynopsis || '').replace(/^no\s+(resource\s+)?change[;:.]\s*/i, '');
  const summary = cleanSummary || '(no summary)';
  const headline = cleanSynopsis.slice(0, 140);
  const pdfHref = bestPdfLink(f);
  const hasText = f.news_text;
  const newBadge = isNew ? '<span class="badge b-new" style="margin-left:4px">NEW</span>' : '';

  // Format date more legibly: "12 Mar 2026"
  let dateFmt = f.filing_date.slice(0,10);
  try {
    const d = new Date(f.filing_date.slice(0,10));
    dateFmt = d.toLocaleDateString('en-GB', {day:'numeric', month:'short', year:'numeric'});
  } catch(e) {}

  // Check if release mentions a technical report - look for matched NI43-101 in currentData
  let techLinks = '';
  if (isChanged && currentData) {
    const techReports = (currentData.filings_by_category?.['NI43-101'] || [])
      .filter(t => {
        const dt = (t.doc_type||'').toUpperCase();
        return dt.includes('TECHNICAL_REPORT') && !dt.includes('CONSENT') && !dt.includes('CERTIFICATE');
      })
      .filter(t => {
        // Match NI43-101 filed 0-3 days before the news release (same-day + Fri→Mon patterns)
        const tDate = new Date(t.filing_date.slice(0,10));
        const fDate = new Date(f.filing_date.slice(0,10));
        const diffDays = (fDate - tDate) / 86400000;
        return diffDays >= 0 && diffDays <= 3;
      })
      .sort((a, b) => new Date(b.filing_date) - new Date(a.filing_date))
      .slice(0, 1); // Only attach the most recent matching NI43-101
    if (techReports.length) {
      techLinks = techReports.map(t => {
        const href = bestPdfLink(t);
        return href ? `<a class="ni-pdf" href="${href}" target="_blank" onclick="event.stopPropagation()" style="background:rgba(99,102,241,.1);padding:1px 6px;border-radius:3px;">📄 NI43-101 ↗</a>` : '';
      }).filter(Boolean).join(' ');
    }
  }

  const dateKey = f.filing_date.slice(0,10);
  const hasHtml = f.news_html_path ? true : false;
  const isReadable = hasText || hasHtml;
  const clickHandler = isReadable ? `onclick="openNewsModal('${esc(currentSymbol)}','${dateKey}','${esc(dateFmt)}',${hasHtml})"` : '';
  const cursorStyle = isReadable ? 'cursor:pointer;' : '';

  return `<div class="news-item ${cls}${isNew?' is-new':''}" ${clickHandler} style="${cursorStyle}">
    <div class="ni-main">
      <span class="ni-date">${dateFmt}${newBadge}</span>
      <span class="ni-flag">${icon}</span>
      <div class="ni-content">
        <div style="display:flex;align-items:center;gap:8px;min-width:0;flex-wrap:nowrap;">
          <div class="ni-summary" style="flex-shrink:0;max-width:50%;">${esc(summary)}</div>
          ${displayProject ? `<span style="background:rgba(239,68,68,.15);color:#f87171;font-size:10px;font-weight:600;padding:1px 6px;border-radius:3px;white-space:nowrap;flex-shrink:0;">${esc(displayProject)}</span>` : ''}
          ${techLinks ? `<span style="flex-shrink:0;">${techLinks}</span>` : ''}
          ${headline ? `<div style="color:#94a3b8;font-size:10px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;flex:1;min-width:0;">${esc(headline)}</div>` : ''}
        </div>
      </div>
    </div>
  </div>`;
}

function openNewsModal(symbol, dateKey, dateFmt, hasHtml) {
  const modal = document.getElementById('newsModal');
  const frame = document.getElementById('modalFrame');
  const title = document.getElementById('modalTitle');
  title.textContent = `${symbol} — ${dateFmt}`;

  // Update copy-link button with the R2 URL if available
  const copyBtn = document.getElementById('modalCopyBtn');
  const niBtn   = document.getElementById('modalNiBtn');
  const allFilings = Object.values(currentData?.filings_by_category || {}).flat();
  const filing = allFilings.find(f => f.filing_date && f.filing_date.slice(0,10) === dateKey);
  // Prefer HTML R2 link, fall back to PDF R2 link for PDF-fallback releases
  const r2url = (filing && (filing.news_html_r2_url || filing.r2_url)) || '';

  // Find matched NI43-101 (filed 0-3 days before) and show its R2 link
  if (niBtn) {
    const fDate = new Date(dateKey);
    const matchedNi = (currentData?.filings_by_category?.['NI43-101'] || [])
      .find(t => {
        const dt = (t.doc_type||'').toUpperCase();
        if (!dt.includes('TECHNICAL_REPORT') || dt.includes('CONSENT') || dt.includes('CERTIFICATE')) return false;
        const tDate = new Date(t.filing_date.slice(0,10));
        const diff = (fDate - tDate) / 86400000;
        return diff >= 0 && diff <= 3;
      });
    const niLink = matchedNi ? bestPdfLink(matchedNi) : '';
    if (niLink) {
      niBtn.href = niLink;
      niBtn.style.display = 'inline-block';
    } else {
      niBtn.style.display = 'none';
    }
  }
  if (copyBtn) {
    if (r2url) {
      copyBtn.style.display = 'inline-block';
      copyBtn.classList.remove('copied');
      copyBtn.textContent = '🔗 Copy link';
      copyBtn.onclick = (e) => {
        e.stopPropagation();
        // Use input selection trick - works on HTTP without clipboard permissions
        const inp = document.createElement('input');
        inp.value = r2url;
        inp.style.cssText = 'position:fixed;top:0;left:0;opacity:0;';
        document.body.appendChild(inp);
        inp.focus();
        inp.select();
        inp.setSelectionRange(0, 99999);
        const ok = document.execCommand('copy');
        document.body.removeChild(inp);
        if (ok || true) { // always show feedback regardless
          copyBtn.textContent = '✓ Copied!';
          copyBtn.classList.add('copied');
          setTimeout(() => {
            copyBtn.textContent = '🔗 Copy link';
            copyBtn.classList.remove('copied');
          }, 2000);
        }
      };
    } else {
      copyBtn.style.display = 'none';
    }
  }

  // Reset iframe fully before loading new content to prevent stale cache
  frame.removeAttribute('srcdoc');
  frame.src = 'about:blank';

  // Use setTimeout to let the blank load flush before setting real content
  setTimeout(() => {
    if (hasHtml) {
      frame.src = `/api/news-html/${symbol}/${dateKey}`;
    } else {
      // Fallback: plain text in a styled srcdoc
      const filing = Object.values(currentData?.filings_by_category || {}).flat()
        .find(f => f.filing_date.slice(0,10) === dateKey && f.news_text);
      const text = filing?.news_text || '(no text available)';
      frame.srcdoc = `<!DOCTYPE html><html><head><meta charset="UTF-8">
        <style>body{background:#0f1117;color:#e2e8f0;font-family:-apple-system,sans-serif;font-size:13px;line-height:1.6;padding:20px 28px;white-space:pre-wrap;word-break:break-word;}</style>
        </head><body>${text.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;')}</body></html>`;
    }
  }, 50);

  modal.style.display = 'flex';
}

function closeNewsModal() {
  const modal = document.getElementById('newsModal');
  const frame = document.getElementById('modalFrame');
  const niBtn = document.getElementById('modalNiBtn');
  modal.style.display = 'none';
  frame.src = '';
  frame.srcdoc = '';
  if (niBtn) niBtn.style.display = 'none';
}



const _expandedLists = new Set();

function expandDocList(bodyId) {
  _expandedLists.add(bodyId);
  renderAll(currentData);
}

function toggleNone(hdr) {
  const body = hdr.nextElementSibling;
  const open = body.classList.toggle('on');
  hdr.querySelector('span').textContent = open ? '▼' : '▶';
}

// ---------------------------------------------------------------------------
// PDF link helper - prefer permanent URLs
// ---------------------------------------------------------------------------
function bestPdfLink(f) {
  if (!f) return '';
  // 1. R2 public URL - no login, works from any machine
  if (f.r2_url) return f.r2_url;
  // 2. SEDAR+ permanent document URL - also public, no login
  if (f.sedar_url && f.sedar_url.includes('document.html')) return f.sedar_url;
  // 3. Serve local file through Flask (works on LAN/Tailscale, requires BAR on)
  if (f.pdf_path) return `/api/pdf/${currentSymbol}/${encodeURIComponent(f.pdf_path.split('\\').pop())}`;
  // 4. Stockwatch URL - requires Stockwatch login (avoid)
  // if (f.pdf_url && f.pdf_url.includes('stockwatch.com')) return f.pdf_url;
  return '';
}

// ---------------------------------------------------------------------------
// Run trigger + progress polling
// ---------------------------------------------------------------------------
async function triggerRun() {
  if (!currentSymbol) return;
  const btn = document.getElementById('runBtn');
  btn.disabled = true;
  document.getElementById('resetBtn').disabled = true;
  btn.innerHTML = '<span class="spin"></span>';
  try {
    const res = await fetch(`/api/run/${currentSymbol}`, { method:'POST' });
    if (!res.ok) { alert((await res.json()).error||'Failed'); btn.disabled=false; document.getElementById('resetBtn').disabled=false; btn.textContent='▶ Update'; return; }
    showProg(true);
    startPolling();
  } catch(e) { alert('Error'); btn.disabled=false; document.getElementById('resetBtn').disabled=false; btn.textContent='▶ Update'; }
}

async function triggerReset() {
  if (!currentSymbol) return;
  const confirmed = confirm(
    `Reset ${currentSymbol}?\n\nThis will delete all scraped data and re-run a full scrape from scratch.\n\nThis cannot be undone.`
  );
  if (!confirmed) return;
  const runBtn   = document.getElementById('runBtn');
  const resetBtn = document.getElementById('resetBtn');
  runBtn.disabled   = true;
  resetBtn.disabled = true;
  resetBtn.innerHTML = '<span class="spin"></span>';
  try {
    const res = await fetch(`/api/reset/${currentSymbol}`, { method:'POST' });
    if (!res.ok) {
      alert((await res.json()).error || 'Reset failed');
      runBtn.disabled   = false;
      resetBtn.disabled = false;
      resetBtn.textContent = '\u21ba Reset';
      return;
    }
    resetBtn.textContent = '\u21ba Reset';
    showProg(true);
    startPolling();
  } catch(e) {
    alert('Error');
    runBtn.disabled   = false;
    resetBtn.disabled = false;
    resetBtn.textContent = '\u21ba Reset';
  }
}

function showProg(on) { /* progress bar hidden - stage shown in banner */ }

function startPolling() {
  if (pollTimer) clearInterval(pollTimer);
  showProg(true);
  pollTimer = setInterval(pollStatus, 800);
  pollStatus();
}

async function pollStatus() {
  if (!currentSymbol) return;
  try {
    const res  = await fetch(`/api/run-status/${currentSymbol}`);
    const data = await res.json();
    const p    = data.progress;

    document.getElementById('progLabel').textContent = `Stage ${p.stage}/${p.total_stages}: ${p.stage_label}`;
    document.getElementById('progPct').textContent   = `${p.pct}%`;
    document.getElementById('progFill').style.width  = `${p.pct}%`;
    document.getElementById('progDetail').textContent = p.detail || '';

    if (logExpanded && data.log_lines) {
      const lb = document.getElementById('logBox');
      lb.innerHTML = data.log_lines.map(l => esc(l)).join('\n');
      lb.scrollTop = lb.scrollHeight;
    }

    if (!data.is_running && (p.status === 'done' || p.status === 'error')) {
      clearInterval(pollTimer); pollTimer = null;
      if (p.status === 'done') {
        document.getElementById('progLabel').textContent = '✓ Complete';
        document.getElementById('progFill').style.background = 'var(--green)';
        setTimeout(() => { loadCompany(currentSymbol); setTimeout(()=>showProg(false),3000); }, 1000);
      } else {
        document.getElementById('progLabel').textContent = '✗ Error';
        document.getElementById('progFill').style.background = 'var(--red)';
      }
      const btn = document.getElementById('runBtn');
      btn.disabled = false; btn.textContent = '▶ Update';
      document.getElementById('resetBtn').disabled = false;
    }
  } catch(e) {}
}

function toggleLog() {
  logExpanded = !logExpanded;
  document.getElementById('logBox').classList.toggle('on', logExpanded);
  document.getElementById('logTgl').textContent = logExpanded ? 'Hide log ▲' : 'Show log ▼';
}

// ---------------------------------------------------------------------------
// Global running state
// ---------------------------------------------------------------------------
function fmtElapsed(seconds) {
  if (seconds < 60) return `${Math.round(seconds)}s`;
  const m = Math.floor(seconds / 60), s = Math.round(seconds % 60);
  return s > 0 ? `${m}m ${s}s` : `${m}m`;
}

async function checkGlobalRunning() {
  try {
    const res  = await fetch('/api/running');
    const data = await res.json();
    const runs = data.running || {};
    const meta = data.batch_meta || {};
    const symbols = Object.keys(runs);
    const banner  = document.getElementById('globalRunBanner');
    const bannerText = document.getElementById('globalRunText');
    const runBtn = document.getElementById('runBtn');

    const resetBtn = document.getElementById('resetBtn');
    if (symbols.length === 0) {
      banner.style.display = 'none';
      if (runBtn && !runBtn.querySelector('.spin')) {
        runBtn.disabled = false;
        if (!runBtn.classList.contains('onboard')) runBtn.textContent = '▶ Update';
      }
      if (resetBtn && !resetBtn.querySelector('.spin')) {
        // Only re-enable Reset if company is actually onboarded (run btn not in onboard mode)
        if (!document.getElementById('runBtn').classList.contains('onboard')) {
          resetBtn.disabled = false;
          resetBtn.textContent = '↺ Reset';
        }
      }
      return;
    }

    banner.style.display = 'flex';
    if (runBtn) runBtn.disabled = true;
    if (resetBtn) resetBtn.disabled = true;

    const isCurrentSymbol = currentSymbol && runs[currentSymbol];
    if (isCurrentSymbol && !pollTimer) startPolling();

    // Build banner text
    let text = '';
    const total = meta.total || symbols.length;
    const completed = meta.completed || 0;
    const current = meta.current || symbols[0] || '';
    const startedAt = meta.started_at || (runs[symbols[0]] || {}).started_at || '';

    // Time estimates
    let timeStr = '';
    if (startedAt && completed > 0) {
      const elapsed = (Date.now() - new Date(startedAt).getTime()) / 1000;
      const secPerCompany = elapsed / completed;
      const remaining = (total - completed) * secPerCompany;
      timeStr = ` · ${fmtElapsed(elapsed)} elapsed · ~${fmtElapsed(remaining)} remaining`;
    } else if (startedAt) {
      const elapsed = (Date.now() - new Date(startedAt).getTime()) / 1000;
      timeStr = ` · ${fmtElapsed(elapsed)} elapsed`;
    }

    if (total > 1) {
      // Batch run
      text = `Batch run: ${current} (${completed}/${total} done)${timeStr}`;
    } else {
      // Single company run - fetch stage detail
      const sym = current || symbols[0];
      try {
        const sr = await fetch(`/api/run-status/${sym}`);
        const sd = await sr.json();
        const p = sd.progress || {};
        if (p.stage && p.total_stages && p.stage_label) {
          text = `${sym} · Stage ${p.stage}/${p.total_stages}: ${p.stage_label}${timeStr}`;
        } else {
          text = `Running ${sym}${timeStr}`;
        }
      } catch(e) {
        text = `Running ${sym}${timeStr}`;
      }
    }

    bannerText.textContent = text;
  } catch(e) {}
}

// ---------------------------------------------------------------------------
// Util
// ---------------------------------------------------------------------------
function fmtDate(s) {
  if (!s || s === '—') return s || '—';
  try {
    const d = new Date(s.slice(0, 10));
    return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
  } catch(e) { return s; }
}

function esc(s) {
  if (!s) return '';
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
