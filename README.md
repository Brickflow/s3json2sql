# s3json2sql

This library fetches files from an s3 bucket and parses them as JSON log 
entries (where each row is a JSON object to parse). 

## Usage
```javascript
    var s3json2sql = require('s3json2sql');
    s3json2sql({
      s3: {
        s3Options: {
          accessKeyId: '...',
          secretAccessKey: '...'
        },
        bucket: 'brickflow-logs'
      },
      sql: {
        uri: 'mysql://root:password@127.0.0.1/logs'
      },
      tmpDir: __dirname + '/tmp'
    });

```

Note that this utility is aimed to be run by ``forever`` or similar.