'use strict';

const { parentPort } = require('worker_threads');
const fs = require('fs');
const nodePath = require('path');

// ═══════════════════════════════════════════════════════════
//  Compute Worker — CPU-bound tasks offloaded from main thread
//  • Diff (Myers + char-level)
//  • CSV parsing (FSM + encoding detection + delimiter detection)
//  • Migration (text matching)
//  • Duplicate detection
// ═══════════════════════════════════════════════════════════

// ── HTML escaping ─────────────────────────────────────────
function escHtml(s) {
  return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// ── Myers Diff (LCS-based) ────────────────────────────────

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
      dp[i][j] = a[i] === b[j] ? dp[i + 1][j + 1] + 1 : Math.max(dp[i + 1][j], dp[i][j + 1]);
    }
  }

  const edits = [];
  let i = 0, j = 0;
  while (i < n && j < m) {
    if (a[i] === b[j])                  { edits.push({ type: 'equal', aIdx: i, bIdx: j }); i++; j++; }
    else if (dp[i + 1][j] >= dp[i][j + 1]) { edits.push({ type: 'delete', aIdx: i }); i++; }
    else                                { edits.push({ type: 'insert', bIdx: j }); j++; }
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

function charDiff(lineA, lineB) {
  const a = [...lineA], b = [...lineB];
  const n = a.length, m = b.length;
  if (n * m > 100000) {
    return {
      htmlA: '<mark class="compare-char-del">' + escHtml(lineA) + '</mark>',
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
  let ci = 0, cj = 0;
  while (ci < n && cj < m) {
    if (a[ci] === b[cj])                    { ops.push({ type: 'eq', a: a[ci], b: b[cj] }); ci++; cj++; }
    else if (dp[ci + 1][cj] >= dp[ci][cj + 1]) { ops.push({ type: 'del', a: a[ci] }); ci++; }
    else                                     { ops.push({ type: 'ins', b: b[cj] }); cj++; }
  }
  while (ci < n) { ops.push({ type: 'del', a: a[ci] }); ci++; }
  while (cj < m) { ops.push({ type: 'ins', b: b[cj] }); cj++; }

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

// ── CSV Parsing (FSM — handles multiline fields) ──────────

const _ENC_UTF8 = 'utf-8';
const _ENC_UTF8BOM = 'utf-8-bom';
const _ENC_UTF16LE = 'utf-16le';
const _ENC_UTF16BE = 'utf-16be';
const _ENC_LATIN1 = 'latin1';

function detectEncoding(buf) {
  if (buf.length >= 3 && buf[0] === 0xEF && buf[1] === 0xBB && buf[2] === 0xBF) {
    return { encoding: _ENC_UTF8BOM, bomBytes: 3 };
  }
  if (buf.length >= 2 && buf[0] === 0xFF && buf[1] === 0xFE) {
    return { encoding: _ENC_UTF16LE, bomBytes: 2 };
  }
  if (buf.length >= 2 && buf[0] === 0xFE && buf[1] === 0xFF) {
    return { encoding: _ENC_UTF16BE, bomBytes: 2 };
  }
  if (isValidUtf8(buf)) {
    return { encoding: _ENC_UTF8, bomBytes: 0 };
  }
  return { encoding: _ENC_LATIN1, bomBytes: 0 };
}

function isValidUtf8(buf) {
  const limit = Math.min(buf.length, 200 * 1024);
  for (let i = 0; i < limit;) {
    const b = buf[i];
    if (b <= 0x7F) { i++; continue; }
    let extra;
    if ((b & 0xE0) === 0xC0) extra = 1;
    else if ((b & 0xF0) === 0xE0) extra = 2;
    else if ((b & 0xF8) === 0xF0) extra = 3;
    else return false;
    if (i + extra >= limit) break;
    for (let j = 1; j <= extra; j++) {
      if ((buf[i + j] & 0xC0) !== 0x80) return false;
    }
    i += 1 + extra;
  }
  return true;
}

function decodeBuffer(buf, enc) {
  switch (enc.encoding) {
    case _ENC_UTF8BOM:
      return buf.slice(enc.bomBytes).toString('utf-8');
    case _ENC_UTF16LE:
      return buf.slice(enc.bomBytes).toString('utf16le');
    case _ENC_UTF16BE: {
      const data = Buffer.from(buf.slice(enc.bomBytes));
      for (let i = 0; i + 1 < data.length; i += 2) {
        const tmp = data[i]; data[i] = data[i + 1]; data[i + 1] = tmp;
      }
      return data.toString('utf16le');
    }
    case _ENC_LATIN1:
      return buf.toString('latin1');
    default:
      return buf.toString('utf-8');
  }
}

function parseCsvFull(text, delim) {
  const rows = [];
  let fields = [];
  let field = '';
  let enclosed = false;
  let startField = true;

  for (let i = 0; i < text.length; i++) {
    const ch = text[i];
    if (ch === '"') {
      if (i + 1 < text.length && text[i + 1] === '"') {
        if (enclosed) { field += '"'; i++; continue; }
        else { enclosed = true; startField = false; continue; }
      } else {
        if (enclosed) enclosed = false;
        else if (startField) { enclosed = true; startField = false; }
        else field += ch;
        continue;
      }
    }
    if (ch === delim && !enclosed) {
      fields.push(field); field = ''; startField = true; continue;
    }
    if ((ch === '\n' || ch === '\r') && !enclosed) {
      if (ch === '\r' && i + 1 < text.length && text[i + 1] === '\n') i++;
      fields.push(field); rows.push(fields); fields = []; field = ''; startField = true; continue;
    }
    field += ch; startField = false;
  }
  if (field || fields.length > 0) { fields.push(field); rows.push(fields); }
  return rows;
}

function csvQuoteField(val, delim) {
  const s = String(val);
  if (s.includes(delim) || s.includes('"') || s.includes('\n') || s.includes('\r')) {
    return '"' + s.replace(/"/g, '""') + '"';
  }
  return s;
}

function rowsToLines(rows, delim) {
  return rows.map(fields => fields.map(f => csvQuoteField(f, delim)).join(delim));
}

function detectCsvDelimiter(text) {
  const physLines = text.split('\n').filter(l => l.trim()).slice(0, 10);
  if (physLines.length === 0) return ',';
  const candidates = [
    { delim: ',', penalty: 1.0 },
    { delim: ';', penalty: 1.0 },
    { delim: '\t', penalty: 1.0 },
    { delim: '|', penalty: 0.7 },
    { delim: ':', penalty: 0.7 },
  ];
  let best = ',', bestScore = -1;
  for (const { delim, penalty } of candidates) {
    const probeText = physLines.join('\n');
    const rows = parseCsvFull(probeText, delim);
    if (rows.length === 0) continue;
    const counts = rows.map(r => r.length);
    const maxCols = Math.max(...counts);
    if (maxCols < 2) continue;
    const shorterRows = counts.filter(c => c < maxCols).length;
    const consistencyScore = (rows.length - shorterRows) / rows.length;
    const score = (consistencyScore * 1000 + maxCols) * penalty;
    if (score > bestScore) { bestScore = score; best = delim; }
  }
  return best;
}

// ── Migration (text matching) ─────────────────────────────

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

function readTxtLines(filePath) {
  const raw = fs.readFileSync(filePath, 'utf-8');
  const lines = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n').split('\n');
  if (lines.length > 0 && lines[lines.length - 1] === '') lines.pop();
  return lines;
}

function runMigrationDir(oldDir, newDir, uaDir, newFiles) {
  const results = [];
  let totalMatched = 0, totalUnmatched = 0, totalLines = 0;

  for (const filename of newFiles) {
    const oldPath = nodePath.join(oldDir, filename);
    const newPath = nodePath.join(newDir, filename);
    const uaPath = nodePath.join(uaDir, filename);
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
  return { results, totalMatched, totalUnmatched, totalLines };
}

// ── Duplicate detection ───────────────────────────────────

function findDuplicates(targetIndex, targetOrigText, targetOrigSpeakers, entries) {
  const dupes = [];
  for (const e of entries) {
    if (e.index === targetIndex) continue;
    if (e.origText === targetOrigText && e.origSpeakers === targetOrigSpeakers) {
      dupes.push(e.index);
    }
  }
  return dupes;
}

// ── Message handler ───────────────────────────────────────

parentPort.on('message', (msg) => {
  switch (msg.type) {

    // ── Diff (compare modal) ──────────────────────────────
    case 'diff': {
      const rows = buildSideBySideDiff(msg.linesA, msg.linesB);
      parentPort.postMessage({ type: 'diff', requestId: msg.requestId, rows });
      break;
    }

    // ── CSV parse (large file open) ───────────────────────
    case 'parse-csv': {
      try {
        const buf = fs.readFileSync(msg.filePath);
        const enc = detectEncoding(buf);
        const raw = decodeBuffer(buf, enc);
        const normalized = raw.replace(/\r\n/g, '\n').replace(/\r/g, '\n');
        const delim = msg.delimiter || detectCsvDelimiter(normalized);
        const rows = parseCsvFull(normalized, delim);
        const lines = rowsToLines(rows, delim);
        parentPort.postMessage({
          type: 'parse-csv', requestId: msg.requestId,
          lines, delimiter: delim, encoding: enc.encoding, ok: true
        });
      } catch (e) {
        parentPort.postMessage({
          type: 'parse-csv', requestId: msg.requestId,
          ok: false, error: e.message
        });
      }
      break;
    }

    // ── Migration (file mode) ─────────────────────────────
    case 'migrate-file': {
      try {
        const oldLines = readTxtLines(msg.oldPath);
        const newLines = readTxtLines(msg.newPath);
        const uaLines = readTxtLines(msg.uaPath);
        const result = migrateTexts(oldLines, newLines, uaLines);
        parentPort.postMessage({
          type: 'migrate-file', requestId: msg.requestId, ...result, ok: true
        });
      } catch (e) {
        parentPort.postMessage({
          type: 'migrate-file', requestId: msg.requestId, ok: false, error: e.message
        });
      }
      break;
    }

    // ── Migration (dir mode) ──────────────────────────────
    case 'migrate-dir': {
      try {
        const result = runMigrationDir(msg.oldDir, msg.newDir, msg.uaDir, msg.newFiles);
        parentPort.postMessage({
          type: 'migrate-dir', requestId: msg.requestId, ...result, ok: true
        });
      } catch (e) {
        parentPort.postMessage({
          type: 'migrate-dir', requestId: msg.requestId, ok: false, error: e.message
        });
      }
      break;
    }

    // ── Duplicate detection ───────────────────────────────
    case 'find-duplicates': {
      const dupes = findDuplicates(msg.targetIndex, msg.targetOrigText, msg.targetOrigSpeakers, msg.entries);
      parentPort.postMessage({ type: 'find-duplicates', requestId: msg.requestId, dupes });
      break;
    }
  }
});
