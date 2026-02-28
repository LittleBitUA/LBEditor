'use strict';

const { parentPort } = require('worker_threads');

// ── Utility functions (duplicated from renderer.js) ────────
const CYRILLIC_RE = /[\u0400-\u04FF]/;
const LATIN_RE = /[a-zA-Z]/;

function lineIsNeutral(line) {
  const trimmed = line.trim();
  return trimmed.length > 0 && !CYRILLIC_RE.test(trimmed) && !LATIN_RE.test(trimmed);
}

function lineIsTranslated(line, codeWordsSet) {
  if (CYRILLIC_RE.test(line)) return true;
  if (lineIsNeutral(line)) return true;
  if (codeWordsSet && codeWordsSet.size > 0) {
    const trimmed = line.trim();
    if (trimmed && codeWordsSet.has(trimmed)) return true;
  }
  return false;
}

function countWords(text) {
  return text.split(/\s+/).filter(w => w.length > 0).length;
}

function classifyLine(trimmed) {
  const cyrCount = (trimmed.match(/[\u0400-\u04FF]/g) || []).length;
  const latCount = (trimmed.match(/[a-zA-Z]/g) || []).length;
  if (cyrCount === 0 && latCount === 0) return 'neutral';
  if (cyrCount === 0) return 'en';
  if (latCount === 0) return 'ua';
  return cyrCount >= latCount ? 'ua' : 'en';
}

// ── Progress calculation ───────────────────────────────────
function calcProgress(entries, codeWords) {
  let transE = 0, totalE = entries.length, transL = 0, totalL = 0;
  const codeSet = codeWords ? new Set(codeWords) : new Set();
  for (const entry of entries) {
    const lines = Array.isArray(entry.text) ? entry.text : entry.text.split('\n');
    const nonEmpty = lines.filter(l => l.trim());
    totalL += nonEmpty.length;
    const translated = nonEmpty.filter(l => lineIsTranslated(l, codeSet));
    transL += translated.length;
    if (nonEmpty.length > 0 && translated.length === nonEmpty.length) transE++;
  }
  return { transE, totalE, transL, totalL };
}

// ── Extended stats ─────────────────────────────────────────
function calcExtendedStats(entries) {
  let totalEntries = entries.length;
  let totalLines = 0, uaLines = 0, enLines = 0, neutralLines = 0;
  let totalWords = 0, uaWords = 0, enWords = 0;
  let totalChars = 0, uaChars = 0, enChars = 0;

  for (const entry of entries) {
    const lines = Array.isArray(entry.text) ? entry.text : entry.text.split('\n');
    for (const line of lines) {
      const trimmed = line.trim();
      if (!trimmed) continue;
      totalLines++;
      const wc = countWords(trimmed);
      totalWords += wc;
      totalChars += trimmed.length;
      const cls = classifyLine(trimmed);
      if (cls === 'ua') { uaLines++; uaWords += wc; uaChars += trimmed.length; }
      else if (cls === 'neutral') { neutralLines++; }
      else { enLines++; enWords += wc; enChars += trimmed.length; }
    }
  }
  const translatableLines = totalLines - neutralLines;
  const uaPct = translatableLines > 0 ? (uaLines / translatableLines * 100) : 0;
  const enPct = translatableLines > 0 ? (enLines / translatableLines * 100) : 0;

  return {
    totalEntries, totalLines, totalWords, totalChars, neutralLines,
    uaLines, uaWords, uaChars, uaPct,
    enLines, enWords, enChars, enPct,
  };
}

// ── Frequent words scan ────────────────────────────────────
function scanFreqWords(entries, glossaryKeys, minCount, caseSensitive, wholeLine) {
  const freq = new Map();
  const glossarySet = new Set(glossaryKeys || []);

  for (const entry of entries) {
    const textStr = Array.isArray(entry.text) ? entry.text.join('\n') : entry.text;

    if (wholeLine) {
      const lines = textStr.split('\n');
      for (const line of lines) {
        const trimmed = line.trim();
        if (!trimmed) continue;
        const key = caseSensitive ? trimmed : trimmed.toLowerCase();
        const existing = freq.get(key);
        if (existing) existing.count++;
        else freq.set(key, { original: trimmed, count: 1 });
      }
    } else {
      const wordRe = /[A-Za-z][\w'-]*(?:\s+[A-Za-z][\w'-]*)*/g;
      let m;
      while ((m = wordRe.exec(textStr)) !== null) {
        const word = m[0].trim();
        if (word.length < 2) continue;
        if (glossarySet.has(word)) continue;
        const key = caseSensitive ? word : word.toLowerCase();
        const existing = freq.get(key);
        if (existing) existing.count++;
        else freq.set(key, { original: word, count: 1 });
      }
    }
  }

  return [...freq.values()]
    .filter(v => v.count >= minCount)
    .sort((a, b) => b.count - a.count)
    .slice(0, 200);
}

// ── Precompute glossary hints for all entries ────────────
function precomputeNav(entries, glossaryKeys) {
  // Build word-boundary regexes for each glossary key
  const keyRegexes = glossaryKeys.map(k => {
    const escaped = k.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
    return new RegExp('\\b' + escaped + '\\b', 'i');
  });

  const results = new Array(entries.length);
  for (let i = 0; i < entries.length; i++) {
    const e = entries[i];
    const text = Array.isArray(e.text) ? e.text.join('\n') : (e.text || '');
    const found = [];
    for (let k = 0; k < glossaryKeys.length; k++) {
      if (keyRegexes[k].test(text)) found.push(glossaryKeys[k]);
      if (found.length > 5) break;
    }
    results[i] = { index: e.index, count: found.length, names: found.slice(0, 4) };
  }
  return results;
}

// ── Message handler ────────────────────────────────────────
parentPort.on('message', (msg) => {
  switch (msg.type) {
    case 'calc-progress': {
      const result = calcProgress(msg.entries, msg.codeWords);
      parentPort.postMessage({ type: 'calc-progress', requestId: msg.requestId, ...result });
      break;
    }
    case 'calc-stats': {
      const result = calcExtendedStats(msg.entries);
      parentPort.postMessage({ type: 'calc-stats', requestId: msg.requestId, ...result });
      break;
    }
    case 'scan-freq': {
      const result = scanFreqWords(
        msg.entries, msg.glossaryKeys,
        msg.minCount, msg.caseSensitive, msg.wholeLine
      );
      parentPort.postMessage({ type: 'scan-freq', requestId: msg.requestId, words: result });
      break;
    }
    case 'precompute-nav': {
      const results = precomputeNav(msg.entries, msg.glossaryKeys);
      parentPort.postMessage({ type: 'precompute-nav', requestId: msg.requestId, results });
      break;
    }
  }
});
