const assert = require('node:assert/strict');
const test = require('node:test');
const { selectPreviousRelease } = require('./release-notes.cjs');

function release(tag_name, extra = {}) {
  return { tag_name, draft: false, prerelease: false, ...extra };
}

test('previous release is selected numerically, not by API order or tag text', () => {
  const selected = selectPreviousRelease([
    release('v26.10.31.9'),
    release('v26.10.31.11'),
    release('v26.10.31.10'),
    release('v26.10.31.20'),
  ], 'v26.10.31.11', () => true);
  assert.equal(selected.tag_name, 'v26.10.31.10');
});

test('an upstream upgrade can follow a previous base with a higher fork revision', () => {
  const selected = selectPreviousRelease([
    release('v26.9.30.100'),
    release('v26.10.31.99'),
    release('v26.11.1.2'),
  ], 'v26.11.1.1', () => true);
  assert.equal(selected.tag_name, 'v26.10.31.99');
});

test('unrelated release branches cannot hide fork commits from the current history', () => {
  const ancestors = new Set(['v26.10.31.1']);
  const selected = selectPreviousRelease([
    release('v26.10.31.1'),
    release('v26.10.31.2'),
    release('v26.11.1.1'),
  ], 'v26.10.31.3', tag => ancestors.has(tag));
  assert.equal(selected.tag_name, 'v26.10.31.1');
});

test('the first fork release does not use upstream, draft, or prerelease tags as its baseline', () => {
  const selected = selectPreviousRelease([
    release('v26.10.31'),
    release('v26.10.29.1', { draft: true }),
    release('v26.10.29.2', { prerelease: true }),
    release('v26.10.31.1-rc.1'),
  ], 'v26.10.31.1', () => true);
  assert.equal(selected, undefined);
});
