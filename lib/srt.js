'use strict';
// SRT subtitle parsing / serialising.
//
// Pure: everything here takes and returns line arrays, so it is directly
// unit-testable and shared by the renderer (via require) and the test suite.
// The renderer wraps these in entry-aware helpers in src/16-stats.js.

const TIME_RE = /^\s*-?\d{1,4}:\d{1,2}:\d{1,2}[,.]\d{1,3}\s*-->\s*-?\d{1,4}:\d{1,2}:\d{1,2}[,.]\d{1,3}/;

function strip(line) {
  return String(line == null ? '' : line).replace(/^\uFEFF/, '').trim();
}

// A digits-only line is a cue counter only when a timecode follows it —
// otherwise it is subtitle text that happens to be a number ("1868").
function isCounter(lines, i) {
  return /^\d+$/.test(strip(lines[i])) && i + 1 < lines.length && TIME_RE.test(lines[i + 1]);
}

// `raw` is either the full text or an already-split line array. Only the head
// is scanned — a subtitle file shows its first timecode within a few lines.
function looksLikeSrt(raw) {
  if (!raw) return false;
  const head = Array.isArray(raw) ? raw.slice(0, 80) : String(raw).slice(0, 8000).split('\n');
  for (const l of head) { if (TIME_RE.test(l)) return true; }
  return false;
}

// → [{ num, time, text: [lines] }] or null when the content isn't SRT.
// `num` is null for files that omit the counter line.
function parseCues(lines) {
  if (!Array.isArray(lines) || !looksLikeSrt(lines)) return null;
  const cues = [];
  let i = 0;
  while (i < lines.length) {
    if (!strip(lines[i])) { i++; continue; }
    let num = null;
    if (isCounter(lines, i)) { num = lines[i]; i++; }
    if (!TIME_RE.test(lines[i])) return null; // stray content — not a clean SRT
    const time = lines[i];
    i++;
    const text = [];
    while (i < lines.length && strip(lines[i])) {
      if (TIME_RE.test(lines[i]) || isCounter(lines, i)) break; // next cue, blank line missing
      text.push(lines[i]);
      i++;
    }
    cues.push({ num, time, text });
  }
  return cues.length > 0 ? cues : null;
}

function isCueArray(v) {
  return Array.isArray(v) && v.length > 0 && v[0] &&
         typeof v[0].time === 'string' && Array.isArray(v[0].text);
}

// Editor lines: cue texts, one blank line BETWEEN cues (no trailing blank —
// that keeps the blank↔cue mapping unambiguous when the edits come back).
function cuesToEditorLines(cues) {
  const out = [];
  for (let i = 0; i < cues.length; i++) {
    if (i > 0) out.push('');
    for (const t of cues[i].text) out.push(t);
  }
  return out;
}

function cuesToFileLines(cues) {
  const out = [];
  for (let i = 0; i < cues.length; i++) {
    if (i > 0) out.push('');
    if (cues[i].num !== null) out.push(cues[i].num);
    out.push(cues[i].time);
    for (const t of cues[i].text) out.push(t);
  }
  return out;
}

// Split edited editor text back into per-cue blocks on blank lines.
function editorLinesToGroups(editedLines) {
  const groups = [];
  let cur = [];
  for (const line of editedLines) {
    if (!strip(line)) { groups.push(cur); cur = []; }
    else cur.push(line);
  }
  groups.push(cur);
  return groups;
}

// Full write-back on line arrays: original file lines + edited editor lines →
// new file lines, or null when the edit can't be mapped back safely.
function applyEditedLines(fileLines, editedLines) {
  const cues = parseCues(fileLines);
  if (!cues) return null;

  const groups = editorLinesToGroups(editedLines);
  // Trailing blank lines the user (or the editor) left behind aren't extra cues
  while (groups.length > cues.length && groups[groups.length - 1].length === 0) groups.pop();

  // Refuse rather than guess: a changed block count would shift every subtitle
  // onto the wrong timecode.
  if (groups.length !== cues.length) return null;

  for (let i = 0; i < cues.length; i++) cues[i].text = groups[i];

  let trailing = 0;
  while (trailing < fileLines.length && !strip(fileLines[fileLines.length - 1 - trailing])) trailing++;
  const out = cuesToFileLines(cues);
  for (let i = 0; i < trailing; i++) out.push('');
  return out;
}

// ── Subtitle quality metrics ────────────────────────────────
// Reading speed and line length are the two standard constraints on subtitles;
// both need the cue duration, which only the timecode line carries.

// "00:01:02,500" → milliseconds. Returns null for unparseable input.
function timeToMs(t) {
  const m = /(-?)(\d{1,4}):(\d{1,2}):(\d{1,2})[,.](\d{1,3})/.exec(String(t || ''));
  if (!m) return null;
  const ms = Number(m[5].padEnd(3, '0'));
  const total = Number(m[2]) * 3600000 + Number(m[3]) * 60000 + Number(m[4]) * 1000 + ms;
  return m[1] === '-' ? -total : total;
}

// Duration of a cue in milliseconds, or null when the timecode is malformed.
function cueDurationMs(cue) {
  const parts = String(cue && cue.time || '').split('-->');
  if (parts.length !== 2) return null;
  const a = timeToMs(parts[0]), b = timeToMs(parts[1]);
  if (a === null || b === null) return null;
  return b - a;
}

// Characters counted for reading speed: markup and line breaks don't count.
function countChars(textLines) {
  return textLines
    .join(' ')
    .replace(/<[^>]*>/g, '')       // <i>, <b>, <font ...>
    .replace(/\{[^}]*\}/g, '')     // {\an8} style ASS/SSA overrides
    .replace(/\s+/g, ' ')
    .trim().length;
}

// { chars, durationMs, cps, maxLineLen, lineCount } — cps is null when the
// duration is unknown or zero.
function cueMetrics(cue) {
  const chars = countChars(cue.text);
  const durationMs = cueDurationMs(cue);
  const cps = durationMs && durationMs > 0 ? (chars / (durationMs / 1000)) : null;
  let maxLineLen = 0;
  for (const l of cue.text) {
    const len = l.replace(/<[^>]*>/g, '').replace(/\{[^}]*\}/g, '').trim().length;
    if (len > maxLineLen) maxLineLen = len;
  }
  return { chars, durationMs, cps, maxLineLen, lineCount: cue.text.length };
}

module.exports = {
  TIME_RE, strip, isCounter, looksLikeSrt, parseCues, isCueArray,
  cuesToEditorLines, cuesToFileLines, editorLinesToGroups, applyEditedLines,
  timeToMs, cueDurationMs, countChars, cueMetrics,
};
