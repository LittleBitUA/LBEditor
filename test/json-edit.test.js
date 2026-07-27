'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const je = require('../lib/json-edit');

const scanValues = (t) => je.scanStrings(t).map(s => s.value);
const scanPaths = (t) => je.scanStrings(t).map(s => s.path);

test('finds string values but not object keys', () => {
  assert.deepEqual(scanValues('{"key":"value"}'), ['value']);
  assert.deepEqual(scanValues('{"a":"x","b":"y"}'), ['x', 'y']);
});

test('ignores non-string values', () => {
  assert.deepEqual(scanValues('{"a":1,"b":true,"c":null,"d":"only"}'), ['only']);
});

test('records paths for nested objects and arrays', () => {
  assert.deepEqual(scanPaths('{"a":{"b":"x"}}'), ['a.b']);
  assert.deepEqual(scanPaths('{"rows":[{"t":"x"},{"t":"y"}]}'), ['rows.*.t', 'rows.*.t']);
  assert.deepEqual(scanPaths('{"list":["a","b"]}'), ['list.*', 'list.*']);
});

test('decodes escapes when reporting values', () => {
  assert.deepEqual(scanValues('{"a":"line\\nbreak"}'), ['line\nbreak']);
  assert.deepEqual(scanValues('{"a":"quote\\"inside"}'), ['quote"inside']);
  assert.deepEqual(scanValues('{"a":"\\u0439\\u043e"}'), ['йо']);
});

test('a quote inside a key does not confuse key/value tracking', () => {
  assert.deepEqual(scanValues('{"we\\"ird":"value"}'), ['value']);
  assert.deepEqual(scanPaths('{"we\\"ird":"value"}'), ['we"ird']);
});

test('a colon or brace inside a string is not treated as structure', () => {
  assert.deepEqual(scanValues('{"a":"http://x.com/{y}","b":"z"}'), ['http://x.com/{y}', 'z']);
});

// The whole point: bytes outside the edited range must survive.
test('replacing one value leaves the rest of the document byte-identical', () => {
  const src = '{\n\t"keep" : "untouched",\n\t"edit":"old"\n}\n';
  const out = je.replaceMatching(src, s => s.path === 'edit', ['new']);
  assert.equal(out, '{\n\t"keep" : "untouched",\n\t"edit":"new"\n}\n');
});

test('preserves \\uXXXX escaping style when the original used it', () => {
  const src = '{"a":"\\u0439"}';
  assert.equal(je.replaceMatching(src, () => true, ['йо']), '{"a":"\\u0439\\u043e"}');
});

test('keeps raw cyrillic when the original was raw', () => {
  const src = '{"a":"йо"}';
  assert.equal(je.replaceMatching(src, () => true, ['ой']), '{"a":"ой"}');
});

test('escapes characters that must be escaped', () => {
  const src = '{"a":"x"}';
  assert.equal(je.replaceMatching(src, () => true, ['a"b\\c\nd']), '{"a":"a\\"b\\\\c\\nd"}');
});

test('a no-op edit reproduces the document exactly', () => {
  const src = '{\r\n  "a": "one",\r\n  "b": ["two", "three"]\r\n}\r\n';
  const slots = je.scanStrings(src);
  const out = je.spliceStrings(src, slots.map(s => ({ start: s.start, end: s.end, value: s.value })));
  assert.equal(out, src);
});

test('round-trips a document with mixed escaping without drift', () => {
  const src = '{"raw":"привіт","esc":"\\u043f\\u0440\\u0438\\u0432\\u0456\\u0442","tab":"a\\tb"}';
  const slots = je.scanStrings(src);
  const out = je.spliceStrings(src, slots.map(s => ({ start: s.start, end: s.end, value: s.value })));
  assert.equal(out, src);
});

test('refuses when the value count does not match', () => {
  assert.equal(je.replaceMatching('{"a":"x","b":"y"}', () => true, ['only-one']), null);
});

test('rejects overlapping edit ranges instead of corrupting output', () => {
  assert.throws(() => je.spliceStrings('{"a":"xy"}', [
    { start: 5, end: 9, value: 'p' },
    { start: 6, end: 10, value: 'q' },
  ]), /overlapping/);
});

test('handles an unterminated string by returning what it parsed so far', () => {
  // must not hang or throw — the caller falls back to the old path
  assert.doesNotThrow(() => je.scanStrings('{"a":"unterminated'));
});

// The schema write-back only splices when the text scan and the parsed view
// agree on order and count. If these two modules ever drift apart, the
// write-back silently falls back — this test keeps that from going unnoticed.
const paths = require('../lib/schema-paths');

test('text scan order matches extractByPath order for selected paths', () => {
  const src = `{
  "title": "Головна",
  "rows": [
    { "id": "a", "text": "перший", "note": "skip" },
    { "id": "b", "text": "другий", "note": "skip" }
  ],
  "footer": "Низ"
}`;
  const data = JSON.parse(src);
  const textPaths = ['title', 'rows.*.text', 'footer'];

  const wanted = new Set(textPaths);
  const scanned = je.scanStrings(src).filter(s => wanted.has(s.path)).map(s => s.value);

  const extracted = [];
  for (const p of textPaths) for (const v of paths.extractByPath(data, p)) extracted.push(v);

  assert.deepEqual(scanned, extracted);
});

test('a full schema write-back changes only the selected values', () => {
  const src = '{\n  "keep": "не чіпати",\n  "rows": [\n    { "text": "один" },\n    { "text": "два" }\n  ]\n}\n';
  const out = je.replaceMatching(src, s => s.path === 'rows.*.text', ['ONE', 'TWO']);
  assert.equal(out, '{\n  "keep": "не чіпати",\n  "rows": [\n    { "text": "ONE" },\n    { "text": "TWO" }\n  ]\n}\n');
});

test('multiple edits in one pass stay independent', () => {
  const src = '{"a":"1","b":"2","c":"3"}';
  const out = je.replaceMatching(src, s => s.path !== 'b', ['one', 'three']);
  assert.equal(out, '{"a":"one","b":"2","c":"three"}');
});
