'use strict';
module.exports = function getType(value, key) {
  if (key === 's3json2mysqlHash') {
    return {sql: 'VARCHAR(255) NOT NULL UNIQUE', suffix: 'hash'};
  }
  switch (typeof value) {
    case 'number':
      return {sql: 'DOUBLE', suffix: 'd'};
    case 'string':
      return {sql: 'TEXT', suffix: 'txt'};
    case 'object':
      if (value instanceof Date) {
        return {sql: 'DATETIME', suffix: 'dt'};
      }
      throw new Error('Invalid kind of object here' + value);
    default:
      throw new Error('Invalid kind of something here' + value);
  }
};