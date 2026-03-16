  }

  // Load selected dict into table
  switchGlossaryDictView(select.value);
  document.getElementById('gloss-search').value = '';
  if (_glossaryDocked) {
    // Already docked — just ensure visible
    modal.classList.remove('hidden');
  } else {
    overlay.classList.remove('hidden');
    modal.classList.remove('hidden');
  }
}

function switchGlossaryDictView(which) {
  const dict = which === 'project' ? state.projectGlossary : state.globalGlossary;
  populateGlossaryTable(dict);
}

async function importGlossary() {
  if (_dialogBusy) return;
  _dialogBusy = true;
  let filePath;
  try {
    filePath = await ipcRenderer.invoke('dialog:open-file');
  } finally { _dialogBusy = false; }
  if (!filePath) return;

  let imported;
  try {
    const raw = fs.readFileSync(filePath, 'utf-8');
    imported = JSON.parse(raw);
    if (!imported || typeof imported !== 'object' || Array.isArray(imported)) {
      showInfo('Помилка', 'Файл має бути JSON-об\'єктом {"ключ": "переклад"}.');
      return;
    }
  } catch (e) {
    showInfo('Помилка', `Не вдалося прочитати:\n${e.message}`);
    return;
  }

  const which = document.getElementById('gloss-dict-select').value;
  const current = getGlossaryFromTable();
  const importKeys = Object.keys(imported);
  const conflicts = importKeys.filter(k => current[k] && current[k] !== imported[k]);
  const newKeys = importKeys.filter(k => !current[k]);

  if (importKeys.length === 0) {
    showInfo('Імпорт', 'Словник порожній.');
    return;
  }

  // Resolve conflicts
  let resolvedAction = 'ask'; // 'keep' | 'replace' | 'ask'
  const merged = Object.assign({}, current);
  // Add new entries
  for (const k of newKeys) merged[k] = imported[k];

  if (conflicts.length > 0) {
    const conflictResult = await resolveImportConflicts(conflicts, current, imported);
    if (!conflictResult) return; // cancelled
    for (const k of conflicts) {
      if (conflictResult[k] === 'replace') merged[k] = imported[k];
      // else keep current
    }
  }

  // Apply merged to table
  populateGlossaryTable(merged);
  const addedCount = newKeys.length;
  const replacedCount = conflicts.filter(k => merged[k] === imported[k]).length;
  setStatus(`Імпорт: +${addedCount} нових, ${replacedCount} замінено, ${conflicts.length - replacedCount} збережено.`);
}

function resolveImportConflicts(conflictKeys, current, imported) {
  return new Promise((resolve) => {
    // Build conflict resolution UI
    const overlay = document.getElementById('info-overlay');
    const modal = document.getElementById('info-modal');
    const title = document.getElementById('info-title');
    const body = document.getElementById('info-body');

    title.textContent = `Конфлікти (${conflictKeys.length})`;

    let html = '<div style="max-height:350px;overflow-y:auto;margin-bottom:12px;">';
    html += '<table style="width:100%;font-size:12px;border-collapse:collapse;">';
    html += '<tr style="opacity:0.6;text-align:left;"><th style="padding:4px">Ключ</th><th style="padding:4px">Поточний</th><th style="padding:4px">Імпорт</th><th style="padding:4px">Дія</th></tr>';
    for (const k of conflictKeys) {
      html += `<tr style="border-bottom:1px solid var(--border);">`;
      html += `<td style="padding:4px;font-weight:600;">${escHtml(k)}</td>`;
      html += `<td style="padding:4px;color:var(--text-muted);">${escHtml(current[k])}</td>`;
      html += `<td style="padding:4px;color:var(--accent);">${escHtml(imported[k])}</td>`;
      html += `<td style="padding:4px;"><select class="conflict-action" data-key="${escHtml(k)}" style="padding:2px;background:var(--input-bg);color:var(--text);border:1px solid var(--border);border-radius:3px;">`;
      html += `<option value="keep">Залишити</option><option value="replace">Замінити</option></select></td>`;
      html += `</tr>`;
    }
    html += '</table></div>';
    html += '<div style="display:flex;gap:8px;justify-content:flex-end;">';
    html += '<button id="conflict-keep-all" style="padding:4px 10px;font-size:12px;">Залишити всі</button>';
    html += '<button id="conflict-replace-all" style="padding:4px 10px;font-size:12px;">Замінити всі</button>';
    html += '<button id="conflict-apply" class="btn-primary" style="padding:4px 14px;">Застосувати</button>';
    html += '<button id="conflict-cancel" style="padding:4px 10px;">Скасувати</button>';
    html += '</div>';
    body.innerHTML = html;

    function getResult() {
      const result = {};
      for (const sel of body.querySelectorAll('.conflict-action')) {
        result[sel.dataset.key] = sel.value;
      }
      return result;
    }

    function cleanup() {
      overlay.classList.add('hidden');
      modal.classList.add('hidden');
    }

    body.querySelector('#conflict-keep-all').onclick = () => {
      body.querySelectorAll('.conflict-action').forEach(s => s.value = 'keep');
    };
    body.querySelector('#conflict-replace-all').onclick = () => {
      body.querySelectorAll('.conflict-action').forEach(s => s.value = 'replace');
    };
    body.querySelector('#conflict-apply').onclick = () => { cleanup(); resolve(getResult()); };
    body.querySelector('#conflict-cancel').onclick = () => { cleanup(); resolve(null); };
    document.getElementById('info-close').onclick = () => { cleanup(); resolve(null); };
    document.getElementById('info-close-btn').onclick = () => { cleanup(); resolve(null); };

    overlay.classList.remove('hidden');
    modal.classList.remove('hidden');
  });
}

function hideGlossaryModal() {
  if (_glossaryDocked) {
    hideGlossaryDocked();
    return;
  }
  document.getElementById('glossary-overlay').classList.add('hidden');
  document.getElementById('glossary-modal').classList.add('hidden');
}

function populateGlossaryTable(glossary) {
  const tbody = document.getElementById('gloss-tbody');
  tbody.innerHTML = '';
  const sorted = Object.entries(glossary).sort((a, b) => a[0].toLowerCase().localeCompare(b[0].toLowerCase()));
  for (const [orig, trans] of sorted) addGlossaryRow(orig, trans);
  updateGlossaryCount();
}

function addGlossaryRow(orig = '', trans = '') {
  const tbody = document.getElementById('gloss-tbody');
  const tr = document.createElement('tr');
  tr.innerHTML = `<td><input type="text" value="${escHtml(orig)}" spellcheck="false"></td><td><input type="text" value="${escHtml(trans)}" spellcheck="false"></td>`;
  tr.addEventListener('click', () => {
    document.querySelectorAll('#gloss-tbody tr.selected').forEach(r => r.classList.remove('selected'));
    tr.classList.add('selected');
    glossarySelectedRow = Array.from(tbody.children).indexOf(tr);
  });
  tbody.appendChild(tr);
  updateGlossaryCount();
  return tr;
}

function deleteGlossaryRow() {
  const tbody = document.getElementById('gloss-tbody');
  if (glossarySelectedRow >= 0 && glossarySelectedRow < tbody.children.length) {
    tbody.children[glossarySelectedRow].remove();
    glossarySelectedRow = -1;
    updateGlossaryCount();
  }
}

function updateGlossaryCount() {
  document.getElementById('gloss-count').textContent = `${document.getElementById('gloss-tbody').children.length} записів`;
}

function filterGlossaryTable(text) {
  text = text.toLowerCase();
  const tbody = document.getElementById('gloss-tbody');
  for (const tr of tbody.children) {
    const inputs = tr.querySelectorAll('input');
    const orig = (inputs[0].value || '').toLowerCase();
    const trans = (inputs[1].value || '').toLowerCase();
    tr.style.display = (!text || orig.includes(text) || trans.includes(text)) ? '' : 'none';
  }
}

function getGlossaryFromTable() {
  const result = {};
  const tbody = document.getElementById('gloss-tbody');
  for (const tr of tbody.children) {
    const inputs = tr.querySelectorAll('input');
    const orig = (inputs[0].value || '').trim();
    const trans = (inputs[1].value || '').trim();
    if (orig && trans) result[orig] = trans;
  }
  return result;
}

function saveGlossaryFromModal() {
  const which = document.getElementById('gloss-dict-select').value;
  const entries = getGlossaryFromTable();
  if (which === 'project') {
    state.projectGlossary = entries;
    saveGlossary('project');
  } else {
    state.globalGlossary = entries;
    saveGlossary('global');
  }
  hideGlossaryModal();
  updateHighlights();
  const label = which === 'project' ? state.projectDictName : 'Глобальний';
  setStatus(`Словник «${label}» збережено (${Object.keys(entries).length} записів).`);
}

// ─── Diff modal ─────────────────────────────────────────────

function showDiffModal(original, current, title = 'Diff') {
  const overlay = document.getElementById('diff-overlay');
  const modal = document.getElementById('diff-modal');
  document.getElementById('diff-title').textContent = title;
  document.getElementById('diff-content').innerHTML = buildUnifiedDiff(original, current);
  overlay.classList.remove('hidden');
  modal.classList.remove('hidden');
}

function hideDiffModal() {
  document.getElementById('diff-overlay').classList.add('hidden');
  document.getElementById('diff-modal').classList.add('hidden');
}

// ═══════════════════════════════════════════════════════════
//  Unified diff
// ═══════════════════════════════════════════════════════════

function buildUnifiedDiff(orig, curr) {
  const origLines = orig.split('\n');
  const currLines = curr.split('\n');
  if (orig === curr) return '<span class="diff-hunk">(Немає змін)</span>';

  const edits = myersDiff(origLines, currLines);
  let html = `<span class="diff-del">--- Оригінал</span>\n<span class="diff-add">+++ Редаговане</span>\n`;
  const hunks = buildHunks(edits, origLines, currLines, 3);
  for (const hunk of hunks) {
    html += `<span class="diff-hunk">${escHtml(hunk.header)}</span>\n`;
    for (const line of hunk.lines) {
      if (line.startsWith('+'))      html += `<span class="diff-add">${escHtml(line)}</span>\n`;
      else if (line.startsWith('-')) html += `<span class="diff-del">${escHtml(line)}</span>\n`;
      else                           html += escHtml(line) + '\n';
    }
  }
  return html || '<span class="diff-hunk">(Немає змін)</span>';
}

function myersDiff(a, b) {
  const n = a.length, m = b.length;
  if (n === 0 && m === 0) return [];
  if (n === 0) return b.map((_, i) => ({ type: 'insert', bIdx: i }));
  if (m === 0) return a.map((_, i) => ({ type: 'delete', aIdx: i }));
  if (n * m > 25000000) return simpleDiff(a, b);

  const dp = new Array(n + 1);
  for (let i = 0; i <= n; i++) dp[i] = new Uint16Array(m + 1);
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i+1][j+1] + 1 : Math.max(dp[i+1][j], dp[i][j+1]);
    }
  }

  const edits = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j])               { edits.push({ type: 'equal', aIdx: i, bIdx: j }); i++; j++; }
    else if (dp[i+1][j] >= dp[i][j+1]) { edits.push({ type: 'delete', aIdx: i }); i++; }
    else                              { edits.push({ type: 'insert', bIdx: j }); j++; }
  }
  while (i < n) { edits.push({ type: 'delete', aIdx: i }); i++; }
  while (j < m) { edits.push({ type: 'insert', bIdx: j }); j++; }
  return edits;
}

function simpleDiff(a, b) {
  const edits = [];
  for (let i = 0; i < a.length; i++) edits.push({ type: 'delete', aIdx: i });
  for (let j = 0; j < b.length; j++) edits.push({ type: 'insert', bIdx: j });
  return edits;
}

function buildHunks(edits, origLines, currLines, context) {
  const lines = [];
  for (const e of edits) {
    if (e.type === 'equal')      lines.push({ type: ' ', text: origLines[e.aIdx], aLine: e.aIdx, bLine: e.bIdx });
    else if (e.type === 'delete') lines.push({ type: '-', text: origLines[e.aIdx], aLine: e.aIdx, bLine: -1 });
    else                          lines.push({ type: '+', text: currLines[e.bIdx], aLine: -1, bLine: e.bIdx });
  }

  const hunks = [];
  let i = 0;
  while (i < lines.length) {
    if (lines[i].type === ' ') { i++; continue; }
    let start = Math.max(0, i - context);
    let end = i;
    while (end < lines.length) {
      if (lines[end].type !== ' ') { end++; }
      else {
        let nextChange = end;
        while (nextChange < lines.length && lines[nextChange].type === ' ') nextChange++;
        if (nextChange < lines.length && nextChange - end <= context * 2) { end = nextChange + 1; }
        else { end = Math.min(lines.length, end + context); break; }
      }
    }
    const hunkLines = [];
    let aStart = -1, bStart = -1, aCount = 0, bCount = 0;
    for (let k = start; k < end; k++) {
      const l = lines[k];
      if (l.type === ' ' || l.type === '-') { if (aStart === -1) aStart = l.aLine; aCount++; }
      if (l.type === ' ' || l.type === '+') { if (bStart === -1) bStart = l.bLine; bCount++; }
      hunkLines.push(l.type + l.text);
    }
    if (aStart === -1) aStart = 0;
    if (bStart === -1) bStart = 0;
    hunks.push({ header: `@@ -${aStart+1},${aCount} +${bStart+1},${bCount} @@`, lines: hunkLines });
    i = end;
  }
  return hunks;
}

// ═══════════════════════════════════════════════════════════
//  Side-by-side compare (ComparePlus)
// ═══════════════════════════════════════════════════════════

function getEntryCurrentText(idx) {
  const entry = state.entries[idx];
  if (!entry) return '';
  if (state.appMode === 'jojo') return entry.text;
  if (state.appMode === 'other') return entry.text.join('\n');
  return entry.toFlat(state.useSeparator);
}

function charDiff(lineA, lineB) {
  const a = [...lineA], b = [...lineB];
  const n = a.length, m = b.length;
  if (n * m > 100000) {
    return {
      htmlA: '<mark class="compare-char-del">' + escHtml(lineA) + '</mark>',