'use strict';
// renderReleaseNotes lives in the renderer bundle; pull just that function out
// so the Markdown → HTML rules can be tested without Electron.
const { test } = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const src = fs.readFileSync(path.join(__dirname, '..', 'src', '18-init.js'), 'utf8');
const start = src.indexOf('function renderReleaseNotes(md) {');
const end = src.indexOf('function showUpdateModal()');
if (start < 0 || end < 0) throw new Error('renderReleaseNotes not found in src/18-init.js');
const render = new Function(src.slice(start, end) + '; return renderReleaseNotes;')();

test('renders headings as elements, not literal hashes', () => {
  assert.equal(render('## Новий вітальний екран'), '<h3>Новий вітальний екран</h3>');
  assert.ok(!render('## Заголовок').includes('#'));
});

test('renders bold and inline code', () => {
  assert.equal(render('це **картки проєктів**'), '<p>це <strong>картки проєктів</strong></p>');
  assert.equal(render('поле `backup_keep`'), '<p>поле <code>backup_keep</code></p>');
});

test('renders bullet lists', () => {
  assert.equal(render('- один\n- два'), '<ul><li>один</li><li>два</li></ul>');
});

test('nests indented bullets', () => {
  assert.equal(render('- верх\n  - вкладений'), '<ul><li>верх</li><ul><li>вкладений</li></ul></ul>');
});

test('closes lists before the next heading', () => {
  assert.equal(render('- пункт\n\n## Далі'), '<ul><li>пункт</li></ul><h3>Далі</h3>');
});

test('renders a horizontal rule', () => {
  assert.equal(render('---'), '<hr>');
});

test('renders links but only http(s)', () => {
  assert.equal(render('[тут](https://example.com)'), '<p><a href="https://example.com">тут</a></p>');
  assert.equal(render('[ні](javascript:alert(1))'), '<p>[ні](javascript:alert(1))</p>');
});

// The body comes from a network response, so markup in it must never execute.
test('escapes HTML in the source before formatting', () => {
  assert.equal(render('<img src=x onerror=alert(1)>'),
    '<p>&lt;img src=x onerror=alert(1)&gt;</p>');
  assert.equal(render('**<b>жирний</b>**'), '<p><strong>&lt;b&gt;жирний&lt;/b&gt;</strong></p>');
});

test('blank lines do not produce empty paragraphs', () => {
  assert.equal(render('перший\n\n\nдругий'), '<p>перший</p><p>другий</p>');
});

test('handles CRLF input', () => {
  assert.equal(render('## Тест\r\n- пункт\r\n'), '<h3>Тест</h3><ul><li>пункт</li></ul>');
});

test('empty input yields nothing', () => {
  assert.equal(render(''), '');
});
