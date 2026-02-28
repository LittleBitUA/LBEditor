'use strict';

const { parentPort } = require('worker_threads');
const nspell = require('nspell');

// ── State ──────────────────────────────────────────────────
let spellChecker = null;
let glossaryKeys = [];
let glossaryValues = [];
let glossaryValuesJoined = '';
let glossaryRegex = null;
let glossaryKeysCacheStr = '';

const CYRILLIC_RE = /[\u0400-\u04FF]/;

// ── Glossary regex ─────────────────────────────────────────
function rebuildGlossaryRegex() {
  const keyStr = glossaryKeys.join('\x00');
  if (glossaryRegex && glossaryKeysCacheStr === keyStr) return;
  glossaryKeysCacheStr = keyStr;
  if (glossaryKeys.length === 0) { glossaryRegex = null; return; }
  const sorted = [...glossaryKeys].sort((a, b) => b.length - a.length);
  const pattern = sorted.map(t => t.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('|');
  glossaryRegex = new RegExp('\\b(?:' + pattern + ')\\b', 'gi');
}

// ── Spell error check ──────────────────────────────────────
function isSpellError(word) {
  if (!spellChecker) return false;
  if (!word || word.length < 2) return false;
  if (!CYRILLIC_RE.test(word)) return false;
  if (glossaryValuesJoined.includes(word)) return false;
  return !spellChecker.correct(word);
}

// ── Main highlight computation ─────────────────────────────
function computeHighlight(text, settings) {
  const doSpell = settings.spellEnabled && spellChecker !== null;
  const doGloss = settings.glossaryEnabled;

  if (doGloss) rebuildGlossaryRegex();

  // Glossary ranges
  const glossRanges = [];
  if (doGloss && glossaryRegex) {
    glossaryRegex.lastIndex = 0;
    let gm;
    while ((gm = glossaryRegex.exec(text)) !== null) {
      glossRanges.push({ start: gm.index, end: gm.index + gm[0].length });
    }
  }

  // Spell ranges
  const spellRanges = [];
  if (doSpell) {
    const wordRe = /[\u0400-\u04FF\u0027\u2019\u0301]+/g;
    let segment;
    while ((segment = wordRe.exec(text)) !== null) {
      const word = segment[0];
      const wStart = segment.index;
      const wEnd = wStart + word.length;
      const overlapsGloss = glossRanges.some(g => wStart < g.end && wEnd > g.start);
      if (overlapsGloss) continue;
      const cleanWord = word.replace(/[\u0027\u2019]/g, '\u2019');
      if (cleanWord.length >= 2 && isSpellError(cleanWord)) {
        spellRanges.push({ start: wStart, end: wEnd });
      }
    }
  }

  return { glossRanges, spellRanges };
}

// ── Message handler ────────────────────────────────────────
parentPort.on('message', (msg) => {
  switch (msg.type) {
    case 'init': {
      try {
        spellChecker = nspell(msg.affData, msg.dicData);
        parentPort.postMessage({ type: 'ready' });
      } catch (e) {
        parentPort.postMessage({ type: 'error', error: 'nspell init failed: ' + e.message });
      }
      break;
    }
    case 'glossary': {
      glossaryKeys = msg.keys || [];
      glossaryValues = msg.values || [];
      glossaryValuesJoined = glossaryValues.join('\x00');
      glossaryKeysCacheStr = '';
      break;
    }
    case 'highlight': {
      const result = computeHighlight(msg.text, msg.settings);
      parentPort.postMessage({
        type: 'highlight',
        requestId: msg.requestId,
        elementId: msg.elementId,
        glossRanges: result.glossRanges,
        spellRanges: result.spellRanges,
      });
      break;
    }
  }
});
