'use stict';

module.exports = function safeName(table) {
  try {
    return table.replace(/[^A-Za-z0-9_]+/gi, '_');
  } catch(err) {
    throw err;
    return table;
  }
};