'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const csv = require('../lib/csv');

test('splits plain fields', () => {
  assert.deepEqual(csv.splitRow('a,b,c', ','), ['a', 'b', 'c']);
});

test('respects quotes around a delimiter', () => {
  assert.deepEqual(csv.splitRow('a,"b,c",d', ','), ['a', 'b,c', 'd']);
});

test('unescapes doubled quotes inside a quoted field', () => {
  assert.deepEqual(csv.splitRow('a,"say ""hi""",c', ','), ['a', 'say "hi"', 'c']);
});

test('keeps empty fields', () => {
  assert.deepEqual(csv.splitRow('a,,c', ','), ['a', '', 'c']);
  assert.deepEqual(csv.splitRow(',', ','), ['', '']);
});

test('detects the delimiter that gives a consistent column count', () => {
  assert.equal(csv.detectDelimiter(['a;b;c', 'd;e;f']), ';');
  assert.equal(csv.detectDelimiter(['a\tb\tc', 'd\te\tf']), '\t');
  assert.equal(csv.detectDelimiter(['a,b,c', 'd,e,f']), ',');
});

test('treats a unique all-text first row as headers', () => {
  assert.equal(csv.detectHeaders('id,source,target', ','), true);
});

test('does not treat numeric or duplicated first rows as headers', () => {
  assert.equal(csv.detectHeaders('1,2,3', ','), false);
  assert.equal(csv.detectHeaders('a,a,b', ','), false);
  assert.equal(csv.detectHeaders('a,,b', ','), false);
});

test('maps rows onto header names', () => {
  assert.deepEqual(csv.rowsToObjects(['id,text', '1,hello'], ',', true),
    [{ id: '1', text: 'hello' }]);
});

test('falls back to col_N names without headers', () => {
  assert.deepEqual(csv.rowsToObjects(['1,hello'], ',', false),
    [{ col_0: '1', col_1: 'hello' }]);
});

test('missing trailing fields become empty strings', () => {
  assert.deepEqual(csv.rowsToObjects(['a,b,c', '1,2'], ',', true),
    [{ a: '1', b: '2', c: '' }]);
});

test('full parse detects delimiter and headers together', () => {
  assert.deepEqual(csv.parse('id;text\n1;привіт\n2;бувай'), [
    { id: '1', text: 'привіт' },
    { id: '2', text: 'бувай' },
  ]);
});

test('returns null for content that is not tabular', () => {
  assert.equal(csv.parse('single column\nanother row'), null);
  assert.equal(csv.parse('only one line'), null);
  assert.equal(csv.parse(''), null);
});
