'use stict';

module.exports = function safeName(table) {
  return table.replace(/[^A-Za-z0-9_]+/gi, '_');
};