      htmlB: '<mark class="compare-char-add">' + escHtml(lineB) + '</mark>'
    };
  }
  const dp = new Array(n + 1);
  for (let i = 0; i <= n; i++) dp[i] = new Uint16Array(m + 1);
  for (let i = n - 1; i >= 0; i--) {
    for (let j = m - 1; j >= 0; j--) {
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }
  const ops = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j])                  { ops.push({ type: 'eq', a: a[i], b: b[j] }); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) { ops.push({ type: 'del', a: a[i] }); i++; }
    else                                { ops.push({ type: 'ins', b: b[j] }); j++; }
  }
  while (i < n) { ops.push({ type: 'del', a: a[i] }); i++; }
  while (j < m) { ops.push({ type: 'ins', b: b[j] }); j++; }

  let htmlA = '', htmlB = '', delBuf = '', insBuf = '';
  function flush() {
    if (delBuf) { htmlA += '<mark class="compare-char-del">' + escHtml(delBuf) + '</mark>'; delBuf = ''; }
    if (insBuf) { htmlB += '<mark class="compare-char-add">' + escHtml(insBuf) + '</mark>'; insBuf = ''; }
  }
  for (const op of ops) {
    if (op.type === 'eq') { flush(); htmlA += escHtml(op.a); htmlB += escHtml(op.b); }
    else if (op.type === 'del') delBuf += op.a;
    else insBuf += op.b;
  }
  flush();
  return { htmlA, htmlB };
}

function buildSideBySideDiff(linesA, linesB) {
  const edits = myersDiff(linesA, linesB);
  const rows = [];
  for (const edit of edits) {
    if (edit.type === 'equal') {
      rows.push({
        left:  { num: edit.aIdx + 1, html: escHtml(linesA[edit.aIdx]), type: 'equal' },
        right: { num: edit.bIdx + 1, html: escHtml(linesB[edit.bIdx]), type: 'equal' }
      });
    } else if (edit.type === 'delete') {
      rows.push({
        left:  { num: edit.aIdx + 1, html: escHtml(linesA[edit.aIdx]), type: 'delete' },
        right: { num: '', html: '', type: 'empty' }
      });
    } else if (edit.type === 'insert') {
      rows.push({
        left:  { num: '', html: '', type: 'empty' },
        right: { num: edit.bIdx + 1, html: escHtml(linesB[edit.bIdx]), type: 'insert' }
      });
    }
  }
  // Merge adjacent delete+insert into 'changed' with char-level diff
  for (let i = 0; i < rows.length - 1; i++) {
    if (rows[i].left.type === 'delete' && rows[i].right.type === 'empty' &&
        rows[i + 1].left.type === 'empty' && rows[i + 1].right.type === 'insert') {
      const la = linesA[rows[i].left.num - 1];
      const lb = linesB[rows[i + 1].right.num - 1];
      const cd = charDiff(la, lb);
      rows[i] = {
        left:  { num: rows[i].left.num, html: cd.htmlA, type: 'changed' },
        right: { num: rows[i + 1].right.num, html: cd.htmlB, type: 'changed' }
      };
      rows.splice(i + 1, 1);
    }
  }
  return rows;
}

// ── Compare modal state ──
let _compareDiffs = [];
let _compareDiffIdx = -1;

function showCompareModal(idxA, idxB) {
  const entryA = state.entries[idxA];
  const entryB = state.entries[idxB];
  if (!entryA || !entryB) return;

  const textA = getEntryCurrentText(idxA);
  const textB = getEntryCurrentText(idxB);
  const linesA = textA.split('\n');
  const linesB = textB.split('\n');
  const rows = buildSideBySideDiff(linesA, linesB);

  // Titles
  const nameA = entryA.file || '#' + idxA;
  const nameB = entryB.file || '#' + idxB;
  document.getElementById('compare-title').textContent = 'Порівняння: ' + nameA + ' \u2194 ' + nameB;
  document.getElementById('compare-left-title').textContent = nameA;
  document.getElementById('compare-right-title').textContent = nameB;

  // Render panels
  const leftContent = document.getElementById('compare-left-content');
  const rightContent = document.getElementById('compare-right-content');
  let leftHtml = '', rightHtml = '';
  _compareDiffs = [];

  const classMap = { delete: 'cmp-del', insert: 'cmp-add', changed: 'cmp-changed', empty: 'cmp-empty', equal: '' };

  for (let r = 0; r < rows.length; r++) {
    const row = rows[r];
    const lc = classMap[row.left.type] || '';
    const rc = classMap[row.right.type] || '';
    leftHtml  += '<div class="compare-line ' + lc + '" data-row="' + r + '"><span class="compare-line-num">' + row.left.num + '</span><span class="compare-line-text">' + (row.left.html || '&nbsp;') + '</span></div>';
    rightHtml += '<div class="compare-line ' + rc + '" data-row="' + r + '"><span class="compare-line-num">' + row.right.num + '</span><span class="compare-line-text">' + (row.right.html || '&nbsp;') + '</span></div>';
    if (row.left.type !== 'equal') _compareDiffs.push(r);
  }

  leftContent.innerHTML = leftHtml;
  rightContent.innerHTML = rightHtml;

  // Change log
  const logContent = document.getElementById('compare-log-content');
  let logHtml = '';
  for (const dr of _compareDiffs) {
    const row = rows[dr];
    if (row.left.type === 'delete') {
      const t = linesA[row.left.num - 1] || '';
      const s = t.length > 80 ? t.slice(0, 77) + '\u2026' : t;
      logHtml += '<div class="compare-log-entry compare-log-del" data-row="' + dr + '">\u0420\u044f\u0434\u043e\u043a ' + row.left.num + ': \u0432\u0438\u0434\u0430\u043b\u0435\u043d\u043e \u00ab' + escHtml(s) + '\u00bb</div>';
    } else if (row.right.type === 'insert') {
      const t = linesB[row.right.num - 1] || '';
      const s = t.length > 80 ? t.slice(0, 77) + '\u2026' : t;
      logHtml += '<div class="compare-log-entry compare-log-add" data-row="' + dr + '">\u0420\u044f\u0434\u043e\u043a ' + row.right.num + ': \u0434\u043e\u0434\u0430\u043d\u043e \u00ab' + escHtml(s) + '\u00bb</div>';
    } else if (row.left.type === 'changed') {
      const tA = linesA[row.left.num - 1] || '', tB = linesB[row.right.num - 1] || '';
      const sA = tA.length > 40 ? tA.slice(0, 37) + '\u2026' : tA;
      const sB = tB.length > 40 ? tB.slice(0, 37) + '\u2026' : tB;
      logHtml += '<div class="compare-log-entry compare-log-changed" data-row="' + dr + '">\u0420\u044f\u0434\u043e\u043a ' + row.left.num + ': \u0437\u043c\u0456\u043d\u0435\u043d\u043e \u00ab' + escHtml(sA) + '\u00bb \u2192 \u00ab' + escHtml(sB) + '\u00bb</div>';
    }
  }
  if (_compareDiffs.length === 0) {
    logHtml = '<div class="compare-log-entry" style="color:var(--text-muted)">(\u0424\u0430\u0439\u043b\u0438 \u0456\u0434\u0435\u043d\u0442\u0438\u0447\u043d\u0456)</div>';
  }
  logContent.innerHTML = logHtml;

  // Log entry click → scroll to row
  for (const el of logContent.querySelectorAll('.compare-log-entry[data-row]')) {
    el.addEventListener('click', () => {
      const ri = parseInt(el.dataset.row);
      const di = _compareDiffs.indexOf(ri);
      if (di >= 0) { _compareDiffIdx = di; scrollToCompareRow(ri); updateComparePos(); }
    });
  }

  // Sync scroll between panels
  let syncing = false;
  const syncLeft = () => { if (!syncing) { syncing = true; rightContent.scrollTop = leftContent.scrollTop; syncing = false; } };
  const syncRight = () => { if (!syncing) { syncing = true; leftContent.scrollTop = rightContent.scrollTop; syncing = false; } };
  leftContent.onscroll = syncLeft;
  rightContent.onscroll = syncRight;

  // Navigation init
  _compareDiffIdx = _compareDiffs.length > 0 ? 0 : -1;
  updateComparePos();

  // Show
  document.getElementById('compare-overlay').classList.remove('hidden');
  document.getElementById('compare-modal').classList.remove('hidden');

  if (_compareDiffs.length > 0) {
    setTimeout(() => scrollToCompareRow(_compareDiffs[0]), 100);
  }
}

function hideCompareModal() {
  document.getElementById('compare-overlay').classList.add('hidden');
  document.getElementById('compare-modal').classList.add('hidden');
  _compareDiffs = [];
  _compareDiffIdx = -1;
}

function scrollToCompareRow(rowIdx) {
  const lc = document.getElementById('compare-left-content');
  const rc = document.getElementById('compare-right-content');
  for (const el of lc.querySelectorAll('.cmp-current')) el.classList.remove('cmp-current');
  for (const el of rc.querySelectorAll('.cmp-current')) el.classList.remove('cmp-current');
  const lr = lc.querySelector('[data-row="' + rowIdx + '"]');
  const rr = rc.querySelector('[data-row="' + rowIdx + '"]');
  if (lr) { lr.classList.add('cmp-current'); lr.scrollIntoView({ block: 'center', behavior: 'smooth' }); }
  if (rr) rr.classList.add('cmp-current');
}

function updateComparePos() {
  const el = document.getElementById('compare-pos');
  el.textContent = _compareDiffs.length === 0 ? '0/0' : (_compareDiffIdx + 1) + '/' + _compareDiffs.length;
}

function comparePrev() {
  if (_compareDiffs.length === 0) return;
  _compareDiffIdx = (_compareDiffIdx - 1 + _compareDiffs.length) % _compareDiffs.length;
  scrollToCompareRow(_compareDiffs[_compareDiffIdx]);
  updateComparePos();
}

function compareNext() {
  if (_compareDiffs.length === 0) return;
  _compareDiffIdx = (_compareDiffIdx + 1) % _compareDiffs.length;
  scrollToCompareRow(_compareDiffs[_compareDiffIdx]);
  updateComparePos();
}

// ═══════════════════════════════════════════════════════════
//  Migration (Translation Transfer)
// ═══════════════════════════════════════════════════════════

const _migrate = { mode: 'file', oldLines: null, newLines: null, uaLines: null, result: null,
  oldDir: null, newDir: null, uaDir: null, oldFiles: null, newFiles: null, uaFiles: null, dirResults: null };

function showMigrateModal(mode) {
  _migrate.mode = mode || 'file';
  _migrate.oldLines = null;
  _migrate.newLines = null;
  _migrate.uaLines = null;
  _migrate.result = null;
  _migrate.oldDir = null;
  _migrate.newDir = null;
  _migrate.uaDir = null;
  _migrate.oldFiles = null;
  _migrate.newFiles = null;
  _migrate.uaFiles = null;
  _migrate.dirResults = null;

  const isDir = _migrate.mode === 'dir';
  const iconChar = isDir ? '\uD83D\uDCC1' : '\uD83D\uDCC4';
  const labels = isDir
    ? { old: 'Стара директорія', new: 'Нова директорія', ua: 'Українська директорія' }
    : { old: 'Старий текст', new: 'Новий текст', ua: 'Український текст' };

  // Update modal title
  document.querySelector('#migrate-modal .modal-header h3').textContent =
    isDir ? 'Перенесення (директорії)' : 'Перенесення';

  // Reset slot visuals
  for (const key of ['old', 'new', 'ua']) {
    const slot = document.getElementById('migrate-slot-' + key);
    slot.classList.remove('loaded');
    const icon = slot.querySelector('.migrate-slot-icon');
    icon.textContent = iconChar;
    document.getElementById('migrate-' + key + '-file').textContent = '';
    slot.querySelector('.migrate-slot-label').textContent = labels[key];
  }

  document.getElementById('migrate-run').disabled = true;
  document.getElementById('migrate-results').classList.add('hidden');
  document.getElementById('migrate-save').classList.add('hidden');
  document.getElementById('migrate-preview').innerHTML = '';
  document.getElementById('migrate-stats').textContent = '';

  document.getElementById('migrate-overlay').classList.remove('hidden');
  document.getElementById('migrate-modal').classList.remove('hidden');
}

function hideMigrateModal() {
  document.getElementById('migrate-overlay').classList.add('hidden');
  document.getElementById('migrate-modal').classList.add('hidden');
}

function readTxtLines(filePath) {
  const raw = fs.readFileSync(filePath, 'utf-8');
  const lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  return lines;
}

function loadMigrateSlot(key, filePath) {
  if (_migrate.mode === 'dir') {
    return loadMigrateDirSlot(key, filePath);
  }
  let lines;
  try {
    lines = readTxtLines(filePath);
  } catch (e) {
    showInfo('Помилка', 'Не вдалося прочитати файл:\n' + e.message);
    return;
  }

  if (key === 'old') _migrate.oldLines = lines;
  else if (key === 'new') _migrate.newLines = lines;
  else if (key === 'ua') _migrate.uaLines = lines;

  // Update slot UI
  const slot = document.getElementById('migrate-slot-' + key);
  slot.classList.add('loaded');
  const icon = slot.querySelector('.migrate-slot-icon');
  icon.textContent = '\u2705';
  const fileEl = document.getElementById('migrate-' + key + '-file');
  fileEl.textContent = nodePath.basename(filePath) + ' (' + lines.length + ' рядків)';

  // Enable run button if all 3 loaded
  document.getElementById('migrate-run').disabled =
    !(_migrate.oldLines && _migrate.newLines && _migrate.uaLines);
}

function loadMigrateDirSlot(key, dirPath) {
  let files;
  try {
    files = fs.readdirSync(dirPath).filter(f => f.toLowerCase().endsWith('.txt')).sort();
  } catch (e) {
    showInfo('Помилка', 'Не вдалося прочитати директорію:\n' + e.message);
    return;
  }

  if (files.length === 0) {
    showInfo('Помилка', 'У директорії немає .txt файлів.');
    return;
  }

  if (key === 'old') { _migrate.oldDir = dirPath; _migrate.oldFiles = files; }
  else if (key === 'new') { _migrate.newDir = dirPath; _migrate.newFiles = files; }
  else if (key === 'ua') { _migrate.uaDir = dirPath; _migrate.uaFiles = files; }

  // Update slot UI
  const slot = document.getElementById('migrate-slot-' + key);
  slot.classList.add('loaded');
  const icon = slot.querySelector('.migrate-slot-icon');
  icon.textContent = '\u2705';
  const fileEl = document.getElementById('migrate-' + key + '-file');
  fileEl.textContent = nodePath.basename(dirPath) + ' (' + files.length + ' файлів)';

  // Enable run button if all 3 loaded
  document.getElementById('migrate-run').disabled =
    !(_migrate.oldDir && _migrate.newDir && _migrate.uaDir);
}

function migrateTexts(oldLines, newLines, uaLines) {
  const oldMap = new Map();
  for (let i = 0; i < oldLines.length; i++) {
    const line = oldLines[i];
    if (!oldMap.has(line)) oldMap.set(line, []);
    oldMap.get(line).push(i);
  }

  const result = [];
  let matched = 0, unmatched = 0;
  for (let j = 0; j < newLines.length; j++) {
    const indices = oldMap.get(newLines[j]);
    if (indices && indices.length > 0) {
      const oldIdx = indices.shift();
      result.push({ text: uaLines[oldIdx] !== undefined ? uaLines[oldIdx] : newLines[j], matched: true });
      matched++;
    } else {
      result.push({ text: newLines[j], matched: false });
      unmatched++;
    }
  }
  return { result, matched, unmatched, total: newLines.length };
}

function runMigration() {
  if (_migrate.mode === 'dir') return runMigrationDir();

  if (!_migrate.oldLines || !_migrate.newLines || !_migrate.uaLines) return;

  const { result, matched, unmatched, total } = migrateTexts(_migrate.oldLines, _migrate.newLines, _migrate.uaLines);
  _migrate.result = result;

  // Stats
  document.getElementById('migrate-stats').textContent =
    'Перенесено: ' + matched + '/' + total + ' рядків  (' + unmatched + ' нових)';

  // Preview
  let html = '';
  for (let i = 0; i < result.length; i++) {
    const r = result[i];
    const cls = r.matched ? '' : ' new-line';
    html += '<div class="migrate-line' + cls + '"><span class="migrate-line-num">' + (i + 1) + '</span><span class="migrate-line-text">' + escHtml(r.text) + '</span></div>';
  }
  document.getElementById('migrate-preview').innerHTML = html;

  document.getElementById('migrate-results').classList.remove('hidden');
  document.getElementById('migrate-save').classList.remove('hidden');
}

function runMigrationDir() {
  if (!_migrate.oldDir || !_migrate.newDir || !_migrate.uaDir) return;

  const newFiles = _migrate.newFiles;
  const results = [];
  let totalMatched = 0, totalUnmatched = 0, totalLines = 0;

  for (const filename of newFiles) {
    const oldPath = nodePath.join(_migrate.oldDir, filename);
    const newPath = nodePath.join(_migrate.newDir, filename);
    const uaPath = nodePath.join(_migrate.uaDir, filename);

    const newLines = readTxtLines(newPath);

    if (fs.existsSync(oldPath) && fs.existsSync(uaPath)) {
      const oldLines = readTxtLines(oldPath);
      const uaLines = readTxtLines(uaPath);
      const r = migrateTexts(oldLines, newLines, uaLines);
      results.push({ filename, ...r, status: 'migrated' });
      totalMatched += r.matched;
      totalUnmatched += r.unmatched;
    } else {
      results.push({
        filename,
        result: newLines.map(t => ({ text: t, matched: false })),
        matched: 0, unmatched: newLines.length, total: newLines.length,
        status: 'new'
      });
      totalUnmatched += newLines.length;
    }
    totalLines += newLines.length;
  }

  _migrate.dirResults = results;

  // Stats
  const changedCount = results.filter(r => r.matched > 0).length;
  const skippedCount = results.length - changedCount;
  document.getElementById('migrate-stats').textContent =
    'Файлів: ' + results.length + ' (збережено: ' + changedCount + ', пропущено: ' + skippedCount + ')  |  ' +
    'Рядків: ' + totalMatched + '/' + totalLines + ' перенесено (' + totalUnmatched + ' нових)';

  // Preview — per-file summary
  let html = '';
  for (const r of results) {
    const isSkipped = r.matched === 0;
    const cls = isSkipped ? ' new-line' : '';
    const statusIcon = isSkipped ? '\u2014' : '\u2713';
    html += '<div class="migrate-line' + cls + '">' +
      '<span class="migrate-line-num">' + statusIcon + '</span>' +
      '<span class="migrate-line-text">' + escHtml(r.filename) +
      ' \u2014 ' + r.matched + '/' + r.total + ' рядків' +
      (r.unmatched > 0 ? ' (' + r.unmatched + ' нових)' : '') +
      (isSkipped ? ' [пропущено]' : '') +
      '</span></div>';
  }
  document.getElementById('migrate-preview').innerHTML = html;

  document.getElementById('migrate-results').classList.remove('hidden');
  document.getElementById('migrate-save').classList.remove('hidden');
}

async function saveMigrateResult() {
  if (_migrate.mode === 'dir') return saveMigrateDirResult();

  if (!_migrate.result) return;
  const filePath = await ipcRenderer.invoke('dialog:save-txt', 'migrated.txt');
  if (!filePath) return;
  try {
    const text = _migrate.result.map(r => r.text).join('\n');
    fs.writeFileSync(filePath, text, 'utf-8');
    setStatus('Результат збережено: ' + filePath);
  } catch (e) {
    showInfo('Помилка', 'Не вдалося зберегти:\n' + e.message);
  }
}

async function saveMigrateDirResult() {
  if (!_migrate.dirResults || _migrate.dirResults.length === 0) return;
  const changed = _migrate.dirResults.filter(r => r.matched > 0);
  if (changed.length === 0) {
    showInfo('Перенесення', 'Немає змінених файлів для збереження.');
    return;
  }
  const outDir = await ipcRenderer.invoke('dialog:open-folder');
  if (!outDir) return;
  try {
    for (const r of changed) {
      const text = r.result.map(l => l.text).join('\n');
      fs.writeFileSync(nodePath.join(outDir, r.filename), text, 'utf-8');
    }
    setStatus('Збережено ' + changed.length + '/' + _migrate.dirResults.length + ' змінених файлів у: ' + outDir);
  } catch (e) {
    showInfo('Помилка', 'Не вдалося зберегти:\n' + e.message);
  }
}

function setupBookmarksPanel() {
  document.getElementById('bookmarks-close').addEventListener('click', hideBookmarksPanel);
  document.getElementById('bookmarks-close-btn').addEventListener('click', hideBookmarksPanel);
  document.getElementById('bookmarks-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'bookmarks-overlay') hideBookmarksPanel();
  });
  document.getElementById('bookmarks-clear-all').addEventListener('click', () => {
    state.entryBookmarks = {};
    saveEntryBookmarks();
    forceVirtualRender();
    showBookmarksPanel();
  });
}

function setupMigrateModal() {
  // Close buttons
  document.getElementById('migrate-close').addEventListener('click', hideMigrateModal);
  document.getElementById('migrate-close-btn').addEventListener('click', hideMigrateModal);
  document.getElementById('migrate-overlay').addEventListener('click', (e) => {
    if (e.target.id === 'migrate-overlay') hideMigrateModal();
  });

  // Run & save
  document.getElementById('migrate-run').addEventListener('click', runMigration);
  document.getElementById('migrate-save').addEventListener('click', saveMigrateResult);

  // Toolbar button — default to file mode
  document.getElementById('tb-migrate').addEventListener('click', () => showMigrateModal('file'));

  // Slot click → open file or folder dialog depending on mode
  for (const key of ['old', 'new', 'ua']) {
    const slot = document.getElementById('migrate-slot-' + key);

    slot.addEventListener('click', async () => {
      if (_migrate.mode === 'dir') {
        const dirPath = await ipcRenderer.invoke('dialog:open-folder');
        if (dirPath) loadMigrateSlot(key, dirPath);
      } else {
        const filePath = await ipcRenderer.invoke('dialog:open-txt');
        if (filePath) loadMigrateSlot(key, filePath);
      }
    });

    // Drag & drop on each slot
    slot.addEventListener('dragover', (e) => {
      e.preventDefault();
      e.dataTransfer.dropEffect = 'copy';
      slot.classList.add('dragover');
    });
    slot.addEventListener('dragleave', () => {
      slot.classList.remove('dragover');
    });
    slot.addEventListener('drop', (e) => {
      e.preventDefault();
      e.stopPropagation();
      slot.classList.remove('dragover');
      const file = e.dataTransfer.files[0];
      if (file && file.path) loadMigrateSlot(key, file.path);
    });
  }
}

// ═══════════════════════════════════════════════════════════
//  Entry list
// ═══════════════════════════════════════════════════════════

function entryMatchesFilter(entry, filt) {
  return entry.getSearchIndex().includes(filt);
}

function getEntryMatchSnippet(entry, filt) {
  const textStr = Array.isArray(entry.text) ? entry.text.join('\n') : entry.text;
  const lower = textStr.toLowerCase();
  const pos = lower.indexOf(filt);
  if (pos < 0) return null;
  // Find the line containing the match
  const lineStart = textStr.lastIndexOf('\n', pos) + 1;
  let lineEnd = textStr.indexOf('\n', pos);
  if (lineEnd < 0) lineEnd = textStr.length;
  const line = textStr.substring(lineStart, lineEnd).trim();