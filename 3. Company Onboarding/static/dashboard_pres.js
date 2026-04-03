// Presentation card function to add to dashboard.js

function renderPresentation(state) {
  const el = document.getElementById('presentationBody');
  const card = document.getElementById('cardPresentation');
  
  const presUrl = state.presentation_url || '';
  const localPath = state.presentation_local || '';
  
  if (!presUrl && !localPath) {
    card.style.display = 'none';
    return;
  }
  
  card.style.display = 'block';
  const filename = localPath ? localPath.split('/').pop().split('\\').pop() : 'presentation.pdf';
  const cleanName = filename.replace(/^\d{4}-\d{2}-\d{2}_/, '').replace(/\.(pdf|PDF)$/, '');
  
  let html = `<div class="presentation-inner">
    <div style="font-size:13px;font-weight:700;color:#cbd5e1;margin-bottom:2px;">${esc(cleanName)}</div>
    <div style="font-size:11px;color:var(--muted);">Corporate presentation</div>`;
  
  if (presUrl) {
    html += `
    <a class="aif-link" href="${esc(presUrl)}" target="_blank" style="margin-top:5px;display:inline-block;">📄 Open Presentation ↗</a>`;
  }
  
  html += `</div>`;
  el.innerHTML = html;
}