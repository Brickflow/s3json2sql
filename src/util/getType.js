'use strict';
module.exports = function getType(value, key) {
  if (key === 's3json2mysqlHash') {
    return {sql: 'VARCHAR(255) NOT NULL UNIQUE', suffix: 'hash'};
  }
  switch (typeof value) {
    case 'boolean':
      return {sql: 'BOOLEAN', suffix: 'b'};
    case 'number':
      return {sql: 'DOUBLE', suffix: 'd'};
    case 'string':
      return {sql: 'TEXT', suffix: 'txt'};
    case 'object':
      if (value instanceof Date) {
        return {sql: 'DATETIME', suffix: 'dt'};
      } else if (value instanceof Error) {
        return { sql: 'TEXT', suffix, 'txt'};
      }
      throw new Error('Invalid kind of object here' + value);
    default:
      throw new Error('Invalid kind of something here' + value);
  }
};