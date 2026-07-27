'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const kv = require('../lib/keyvalue');

test('parses a flat key=value file', () => {
  assert.deepEqual(kv.parse('A=1\nB=hello\n'), { A: '1', B: 'hello' });
});

test('keeps everything after the first = in the value', () => {
  assert.deepEqual(kv.parse('Path=C:\\a=b\\c'), { Path: 'C:\\a=b\\c' });
});

test('trims the key but never the value', () => {
  assert.deepEqual(kv.parse('  Key  = value with spaces '), { 'Key': ' value with spaces ' });
});

test('parses [Section] files into an array of section objects', () => {
  const out = kv.parse('[One]\nA=1\n\n[Two]\nB=2\n');
  assert.deepEqual(out, [
    { _section: 'One', A: '1' },
    { _section: 'Two', B: '2' },
  ]);
});

test('joins continuation lines into the previous value', () => {
  assert.deepEqual(kv.parse('Text=first\nsecond\nthird\n'), { Text: 'first\nsecond\nthird' });
});

test('a blank line ends a multi-line value', () => {
  assert.deepEqual(kv.parse('Text=first\nsecond\n\nignored\nNext=2'),
    { Text: 'first\nsecond', Next: '2' });
});

test('comments do not continue a value', () => {
  assert.deepEqual(kv.parse('Text=first\n; a comment\nNext=2'), { Text: 'first', Next: '2' });
  assert.deepEqual(kv.parse('Text=first\n# a comment\nNext=2'), { Text: 'first', Next: '2' });
});

test('a comment ends the value for the lines that follow it too', () => {
  assert.deepEqual(kv.parse('Text=first\n; comment\nstray'), { Text: 'first' });
});

test('a blank line ends the value inside a section too', () => {
  assert.deepEqual(kv.parse('[A]\nT=x\ncont\n\nstray\nU=y'), [
    { _section: 'A', T: 'x\ncont', U: 'y' },
  ]);
});

test('a section header ends a multi-line value', () => {
  assert.deepEqual(kv.parse('[A]\nT=x\ncont\n[B]\nT=y'), [
    { _section: 'A', T: 'x\ncont' },
    { _section: 'B', T: 'y' },
  ]);
});

test('lines before the first section header are dropped', () => {
  assert.deepEqual(kv.parse('orphan=1\n[A]\nB=2'), [{ _section: 'A', B: '2' }]);
});

test('a leading = with no key does not create an entry', () => {
  assert.equal(kv.parse('=novalue'), null);
});

test('returns null when there are no key=value pairs', () => {
  assert.equal(kv.parse('just text\nmore text'), null);
  assert.equal(kv.parse(''), null);
});

test('later duplicate keys win', () => {
  assert.deepEqual(kv.parse('A=1\nA=2'), { A: '2' });
});
