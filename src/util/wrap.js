'use strict';

var pairOf = {
  '(': ')',
  '[': ']',
  '{': '}',
  '<': '>'
};

module.exports = function wrap(payload, prefix, suffix) {
  if (prefix === undefined) {
    throw new TypeError('Prefix required for wrap()');
  }
  return prefix + payload + (suffix || pairOf[prefix] || prefix);
};