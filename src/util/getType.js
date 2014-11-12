'use strict';
module.exports = function getType(value) {
  switch (typeof value) {
    case 'number':
      return {sql: 'DOUBLE', suffix: 'd'};
    case 'string':
      return {sql: 'TEXT', suffix: 'txt'};
    case 'object':
      if (value instanceof Date) {
        return {sql: 'DATETIME', suffix: 'dt'};
      }
      throw new Error('Invalid kind of content here', value);
  }
};