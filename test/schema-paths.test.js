'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const p = require('../lib/schema-paths');

test('reads a nested path', () => {
  assert.deepEqual(p.extractByPath({ a: { b: 'x' } }, 'a.b'), ['x']);
});

test('walks arrays through *', () => {
  const data = { rows: [{ t: 'one' }, { t: 'two' }] };
  assert.deepEqual(p.extractByPath(data, 'rows.*.t'), ['one', 'two']);
});

test('flattens string arrays into separate values', () => {
  assert.deepEqual(p.extractByPath({ text: ['a', 'b'] }, 'text'), ['a', 'b']);
});

test('skips missing keys and non-strings instead of throwing', () => {
  assert.deepEqual(p.extractByPath({ a: 1 }, 'a'), []);
  assert.deepEqual(p.extractByPath({ a: { b: 'x' } }, 'a.missing'), []);
  assert.deepEqual(p.extractByPath(null, 'a'), []);
  assert.deepEqual(p.extractByPath({ a: 'x' }, ''), []);
});

// The invariant the write-back depends on: slots must line up 1:1 with the
// values extractByPath returns, in the same order. If these ever diverge,
// edits land on the wrong fields.
test('writable slots line up 1:1 with extracted values', () => {
  const data = { rows: [{ t: 'one' }, { t: 'two' }], title: ['a', 'b'] };
  for (const path of ['rows.*.t', 'title']) {
    const vals = p.extractByPath(data, path);
    const slots = p.collectWritableSlots(data, path);
    assert.equal(slots.length, vals.length, `count mismatch for ${path}`);
    slots.forEach((s, i) => assert.equal(s.container[s.key], vals[i], `order mismatch for ${path}`));
  }
});

test('writing through a slot updates the source object', () => {
  const data = { rows: [{ t: 'one' }, { t: 'two' }] };
  const slots = p.collectWritableSlots(data, 'rows.*.t');
  slots[1].container[slots[1].key] = 'CHANGED';
  assert.equal(data.rows[1].t, 'CHANGED');
});

test('classifies value types for the schema tree', () => {
  assert.equal(p.valueType('s'), 'string');
  assert.equal(p.valueType(['a']), 'string-array');
  assert.equal(p.valueType([{ a: 1 }]), 'object-array');
  assert.equal(p.valueType([]), 'empty-array');
  assert.equal(p.valueType({}), 'object');
  assert.equal(p.valueType(null), 'null');
  assert.equal(p.valueType(undefined), 'null');
  assert.equal(p.valueType(1), 'number');
  assert.equal(p.valueType(true), 'boolean');
});

// structureSig values are persisted in settings; a format change silently
// stops saved schemas from auto-matching their files.
test('structure signature is stable and key-order independent', () => {
  assert.equal(p.structureSignature({ b: 'x', a: 1 }, 0), p.structureSignature({ a: 1, b: 'x' }, 0));
  assert.equal(p.structureSignature({ a: 'x' }, 0), 'a:string');
  assert.equal(p.structureSignature({ r: [{ t: 'x' }] }, 0), 'r:object[]:{t:string}');
  assert.equal(p.structureSignature({ t: ['a'] }, 0), 't:string[]');
  assert.equal(p.structureSignature({ n: null, u: undefined }, 0), 'n:null,u:null');
});

test('structure signature stops recursing past depth 2', () => {
  const deep = { a: { b: { c: { d: 'x' } } } };
  assert.equal(p.structureSignature(deep, 0), 'a:object:{b:object:{c:object:{}}}');
});

test('structure signature distinguishes different shapes', () => {
  assert.notEqual(p.structureSignature({ a: 'x' }, 0), p.structureSignature({ a: 1 }, 0));
  assert.notEqual(p.structureSignature({ a: 'x' }, 0), p.structureSignature({ b: 'x' }, 0));
});

test('path leaf and depth ignore wildcards', () => {
  assert.equal(p.pathLeaf('rows.*.value'), 'value');
  assert.equal(p.pathLeaf('rows.*'), 'rows');
  assert.equal(p.pathLeaf('a'), 'a');
  assert.equal(p.pathDepth('rows.*.value'), 2);
  assert.equal(p.pathDepth('a'), 1);
  assert.equal(p.pathDepth(''), 0);
});

// The reported bug: Shift-clicking the last `value` used to also select every
// `key` in between.
test('Shift range picks one field out of repeated records', () => {
  const paths = [];
  for (let i = 0; i < 5; i++) paths.push(`MSG_${i}.key`, `MSG_${i}.value`);
  assert.deepEqual(p.shiftRangeIndexes(paths, 1, 7).map(i => paths[i]),
    ['MSG_0.value', 'MSG_1.value', 'MSG_2.value', 'MSG_3.value']);
  // and the same anchored from the other end
  assert.deepEqual(p.shiftRangeIndexes(paths, 7, 1), p.shiftRangeIndexes(paths, 1, 7));
});

test('Shift range stays contiguous on a flat file', () => {
  const flat = ['GREETING', 'FAREWELL', 'YES', 'NO', 'MAYBE'];
  assert.deepEqual(p.shiftRangeIndexes(flat, 1, 3).map(i => flat[i]), ['FAREWELL', 'YES', 'NO']);
});

test('Shift range spanning different field names keeps everything in between', () => {
  const paths = ['M0.key', 'M0.value', 'M1.key', 'M1.value'];
  assert.deepEqual(p.shiftRangeIndexes(paths, 0, 3), [0, 1, 2, 3]);
});

test('Shift range treats the same name at different depths as different fields', () => {
  const paths = ['a.text', 'b.c.text'];
  assert.deepEqual(p.shiftRangeIndexes(paths, 0, 1), [0, 1]);
});
