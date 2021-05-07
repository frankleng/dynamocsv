# dynamocsv

### Example
```javascript
import DdbCsv from 'dynamocsv';
const service = new DdbCsv({
  client: new DynamoDBClient({}),
  target: 'dump.csv',  // (string, WritableStream, async callback)
  input: { tableName: 'ddb_test_table', index: 'awesomeness' },
  format: 'csv', // (csv | json | object)
  query: {
    keyCondExpressionMap: {
      hash: 'test',
      range: { op: '>=', value: 1234 },
    },
    filterExpressionMap: { name: 'frank' }
  },
  rowPredicate: (row, context) => {
     const id = uuidv5(`${row['hash']}_${row['range']}`);
     context.prependHeader('id'); // safe to call for each row, headers are stored as a Set
     return [id, ...row];
  }
});

await service.exec();
```

### Args
+ *client* - An instance of DynamoDBClient, example uses aws-sdk 3, but v2 should also work
+ *target* - file name with extension, or an instance of Node.js WriteStream or async callback. ie. `async (rows) => { const result = await writeToDb(rows); return result.count; }`;
+ *format* - csv (default) | json | object - object requires a writable stream in object mode or use an async callback to capture values.k
+ *input*
    + *tableName* - string (required)
    + *index* - string (optional)
    + *limit* - number (optional, default 2000)
+ *query* - a simplified mapping to generate `KeyConditionExpression` and `FilterExpression`. See example above - key/val pair generates a simple equality condition, use an object to specify operator and value if needed.
    + *keyCondExpressionMap*
    + *filterExpressionMap*
+ *rowPredicate* - `(row, context) => row` - a function that is called for each row. See example above - use the `context` methods to modify headers as needed.
