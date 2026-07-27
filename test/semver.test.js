'use strict';
const { test } = require('node:test');
const assert = require('node:assert');
const { isNewer } = require('../lib/semver');

// A false positive here re-offers the same update forever; a false negative
// means nobody ever gets the release.
test('offers an update for a newer tag', () => {
  assert.equal(isNewer('v1.5.0', '1.4.0'), true);
  assert.equal(isNewer('v1.4.1', '1.4.0'), true);
  assert.equal(isNewer('v2.0.0', '1.9.9'), true);
  assert.equal(isNewer('v1.10.0', '1.9.0'), true);
});

test('does not offer an update for the same or older tag', () => {
  assert.equal(isNewer('v1.5.0', '1.5.0'), false);
  assert.equal(isNewer('v1.4.0', '1.5.0'), false);
  assert.equal(isNewer('v1.9.0', '1.10.0'), false);
});

test('tolerates a missing or present v prefix on either side', () => {
  assert.equal(isNewer('1.5.0', 'v1.4.0'), true);
  assert.equal(isNewer('V1.5.0', '1.4.0'), true);
});

test('treats a missing component as zero', () => {
  assert.equal(isNewer('1.5', '1.4.9'), true);
  assert.equal(isNewer('1.5', '1.5.0'), false);
  assert.equal(isNewer('1.5.1', '1.5'), true);
});

test('a release is newer than its own pre-release', () => {
  assert.equal(isNewer('1.5.0', '1.5.0-beta'), true);
  assert.equal(isNewer('1.5.0-beta', '1.5.0'), false);
});

test('pre-releases order among themselves', () => {
  assert.equal(isNewer('1.5.0-beta.2', '1.5.0-beta.1'), true);
  assert.equal(isNewer('1.5.0-beta.1', '1.5.0-beta.2'), false);
  assert.equal(isNewer('1.5.0-beta', '1.5.0-alpha'), true);
  // a longer pre-release chain ranks above its prefix
  assert.equal(isNewer('1.5.0-beta.1', '1.5.0-beta'), true);
});

test('build metadata is ignored', () => {
  assert.equal(isNewer('1.5.0+build9', '1.5.0'), false);
  assert.equal(isNewer('1.5.1+build9', '1.5.0'), true);
});

test('a whitespace-only tag never claims an update', () => {
  assert.equal(isNewer('   ', '1.4.0'), false);
});

test('garbage input never claims an update', () => {
  assert.equal(isNewer('', '1.4.0'), false);
  assert.equal(isNewer(null, '1.4.0'), false);
  assert.equal(isNewer(undefined, '1.4.0'), false);
});
