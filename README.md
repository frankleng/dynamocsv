# dynamocsv

### Example
```javascript
const service = new ddbCsv({
  client: new DynamoDBClient({}),
  target: 'dump.csv',
  input: { tableName: 'ddb_test_table', index: 'awesomeness' },
  query: {
    keyCondExpressionMap: {
      hash: 'test',
      range: { op: '>=', value: 1234 },
    },
    filterExpressionMap: { name: 'frank' }
  },
});

await service.exec();
```

### Args
+ *client* - An instance of DynamoDBClient, example uses aws-sdk 3, but v2 should also work
+ *target* - file name with extension, or an instance of Node.js WriteStream.
+ *input*
    + *tableName* - string (required)
    + *index* - string (optional)
    + *limit* - number (optional, default 2000)
+ *query* - a simplified mapping to generate `KeyConditionExpression` and `FilterExpression`. See example above - key/val pair generates a simple equality condition, use an object to specify operator and value if needed.
    + *keyCondExpressionMap*
    + *filterExpressionMap*

