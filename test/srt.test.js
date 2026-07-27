'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const srt = require('../lib/srt');

const SAMPLE = [
  '1',
  '00:00:01,000 --> 00:00:04,000',
  'Перший рядок субтитра',
  'другий рядок субтитра',
  '',
  '2',
  '00:00:05,500 --> 00:00:07,200',
  'Одинокий рядок',
  '',
  '3',
  '00:00:08,000 --> 00:00:09,000',
  'Ще один',
  'і ще',
];

test('parses every cue with its counter and timecode', () => {
  const cues = srt.parseCues(SAMPLE);
  assert.equal(cues.length, 3);
  assert.deepEqual(cues[0], {
    num: '1',
    time: '00:00:01,000 --> 00:00:04,000',
    text: ['Перший рядок субтитра', 'другий рядок субтитра'],
  });
  assert.deepEqual(cues[1].text, ['Одинокий рядок']);
});

test('editor view shows only text, one blank line between cues', () => {
  assert.deepEqual(srt.cuesToEditorLines(srt.parseCues(SAMPLE)), [
    'Перший рядок субтитра', 'другий рядок субтитра',
    '',
    'Одинокий рядок',
    '',
    'Ще один', 'і ще',
  ]);
});

// The property that matters most: opening a file and applying without editing
// anything must not touch a single byte.
test('identity round-trip is byte-for-byte', () => {
  const edited = srt.cuesToEditorLines(srt.parseCues(SAMPLE));
  assert.deepEqual(srt.applyEditedLines(SAMPLE, edited), SAMPLE);
});

test('translation keeps counters and timecodes, allows new line counts', () => {
  const out = srt.applyEditedLines(SAMPLE,
    ['A1', 'A2', '', 'B', '', 'C1', 'C2', 'C3']);
  assert.deepEqual(out, [
    '1', '00:00:01,000 --> 00:00:04,000', 'A1', 'A2',
    '', '2', '00:00:05,500 --> 00:00:07,200', 'B',
    '', '3', '00:00:08,000 --> 00:00:09,000', 'C1', 'C2', 'C3',
  ]);
});

test('re-extracting after a write-back returns exactly what was typed', () => {
  const edited = ['A1', 'A2', '', 'B', '', 'C1', 'C2', 'C3'];
  const out = srt.applyEditedLines(SAMPLE, edited);
  assert.deepEqual(srt.cuesToEditorLines(srt.parseCues(out)), edited);
});

test('refuses a changed block count instead of shifting text onto wrong timecodes', () => {
  assert.equal(srt.applyEditedLines(SAMPLE, ['A', '', 'B', '', 'C', '', 'D']), null);
  assert.equal(srt.applyEditedLines(SAMPLE, ['A', '', 'B']), null);
});

test('tolerates trailing blank lines left in the editor', () => {
  const edited = ['A1', 'A2', '', 'B', '', 'C1', 'C2', 'C3'];
  assert.deepEqual(
    srt.applyEditedLines(SAMPLE, [...edited, '', '', '']),
    srt.applyEditedLines(SAMPLE, edited));
});

test('handles files with no counter lines without inventing them', () => {
  const noCount = [
    '00:00:01,000 --> 00:00:02,000', 'aaa',
    '', '00:00:03,000 --> 00:00:04,000', 'bbb',
  ];
  const cues = srt.parseCues(noCount);
  assert.equal(cues.length, 2);
  assert.equal(cues[0].num, null);
  assert.deepEqual(srt.applyEditedLines(noCount, ['xxx', '', 'yyy']), [
    '00:00:01,000 --> 00:00:02,000', 'xxx',
    '', '00:00:03,000 --> 00:00:04,000', 'yyy',
  ]);
});

test('preserves a BOM and accepts dot-separated milliseconds', () => {
  const withBom = ['﻿1', '00:00:01.000 --> 00:00:02.000', 'text'];
  const cues = srt.parseCues(withBom);
  assert.equal(cues.length, 1);
  assert.equal(cues[0].num, '﻿1');
  assert.equal(srt.applyEditedLines(withBom, ['перекладено'])[0], '﻿1');
});

test('a digits-only subtitle line is not mistaken for a counter', () => {
  const cues = srt.parseCues([
    '1', '00:00:01,000 --> 00:00:02,000', 'Рік', '1868',
    '', '2', '00:00:03,000 --> 00:00:04,000', 'ok',
  ]);
  assert.equal(cues.length, 2);
  assert.deepEqual(cues[0].text, ['Рік', '1868']);
});

test('rejects content that is not SRT', () => {
  assert.equal(srt.parseCues(['hello', 'world']), null);
  assert.equal(srt.parseCues(['{', '"a": "b"', '}']), null);
  assert.equal(srt.parseCues(['A=1', 'B=2']), null);
  assert.equal(srt.parseCues([]), null);
});

test('preserves trailing blank lines of the original file', () => {
  const withTrailing = [...SAMPLE, '', ''];
  const edited = srt.cuesToEditorLines(srt.parseCues(withTrailing));
  assert.deepEqual(srt.applyEditedLines(withTrailing, edited), withTrailing);
});

// ── metrics ──
test('timeToMs parses both millisecond separators', () => {
  assert.equal(srt.timeToMs('00:00:01,500'), 1500);
  assert.equal(srt.timeToMs('01:02:03.250'), 3723250);
  assert.equal(srt.timeToMs('nonsense'), null);
});

test('cueMetrics computes duration, cps and longest line', () => {
  const m = srt.cueMetrics({
    time: '00:00:01,000 --> 00:00:03,000',
    text: ['Twelve chars', 'and more'],
  });
  assert.equal(m.durationMs, 2000);
  assert.equal(m.chars, 'Twelve chars and more'.length);
  assert.equal(m.cps, m.chars / 2);
  assert.equal(m.maxLineLen, 12);
  assert.equal(m.lineCount, 2);
});

test('cueMetrics ignores markup when counting', () => {
  const m = srt.cueMetrics({
    time: '00:00:00,000 --> 00:00:01,000',
    text: ['<i>abc</i>', '{\\an8}de'],
  });
  assert.equal(m.chars, 'abc de'.length);
});

test('cueMetrics reports null cps for a malformed or zero-length timecode', () => {
  assert.equal(srt.cueMetrics({ time: 'broken', text: ['x'] }).cps, null);
  assert.equal(srt.cueMetrics({ time: '00:00:01,000 --> 00:00:01,000', text: ['x'] }).cps, null);
});
