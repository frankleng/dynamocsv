import { DynamoDBClient, QueryInput, ScanCommand, ScanCommandInput } from '@aws-sdk/client-dynamodb';
import * as fs from 'fs';
import { FilterExpressionMap, KeyCondExpressionMap, queryTableIndex } from 'dynadash';
import csv from 'papaparse';
import { WriteStream } from 'fs';
import { unmarshall } from '@aws-sdk/util-dynamodb';
import WritableStream = NodeJS.WritableStream;
import { Writable } from 'stream';

const DEFAULT_QUERY_LIMIT = 2000;

type TargetCallback = (rows: string | any[]) => void;

type Params = {
  client: DynamoDBClient;
  input: {
    tableName: ScanCommandInput['TableName'];
    index?: ScanCommandInput['IndexName'];
    limit?: ScanCommandInput['Limit'];
  };
  target: string | WritableStream | TargetCallback;
  format?: 'csv' | 'json' | 'object';
  query?: {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  };
  rowPredicate?: (data: any, context: Dynamocsv) => any;
};

export default class Dynamocsv {
  private readonly input: Params['input'];
  private readonly query: Params['query'];
  private readonly targetStream?: WritableStream;
  private readonly targetCallback?: TargetCallback;
  private readonly rowPredicate: Params['rowPredicate'];
  private readonly format: Params['format'];
  private client: DynamoDBClient;
  private writeCount: number;
  private headers: Set<string>;
  private rows: any[];

  constructor({ client, target, input, query, rowPredicate, format }: Params) {
    this.client = client;
    this.input = { ...input, limit: input.limit || DEFAULT_QUERY_LIMIT };
    this.query = query;
    this.writeCount = 0;
    this.headers = new Set();
    this.rows = [];
    this.rowPredicate = rowPredicate;
    this.format = format || 'csv';

    if (typeof target === 'string') this.targetStream = fs.createWriteStream(target, { flags: 'a' });
    else if (target instanceof WriteStream || target instanceof Writable) this.targetStream = target;
    else if (typeof target === 'function') {
      this.targetStream = undefined;
      this.targetCallback = target;
    }
  }

  private writeToTarget(): void {
    let payload;
    if (this.format === 'json') {
      payload = JSON.stringify(this.rows);
    }
    if (this.format === 'object') {
      payload = this.rows;
    } else if (this.format === 'csv') {
      let csvData = csv.unparse({
        fields: [...this.headers.values()],
        data: this.rows,
      });
      if (this.writeCount > 0) {
        // remove column names after first write chunk.
        csvData = csvData.replace(/(.*\r\n)/, '');
      }
      payload = csvData;
    }

    if (this.targetStream) (this.targetStream as WriteStream).write(payload);
    if (this.targetCallback && payload) this.targetCallback(payload);

    this.writeCount += this.rows.length;
    this.rows = [];
  }

  appendHeader(header: string): Set<string> {
    if (!this.headers.has(header)) this.headers.add(header);
    return this.headers;
  }

  prependHeader(header: string): Set<string> {
    if (!this.headers.has(header)) this.headers = new Set([header, ...this.headers]);
    return this.headers;
  }

  /**
   * @param ExclusiveStartKey
   */
  async exec(
    ExclusiveStartKey?: ScanCommandInput['ExclusiveStartKey'] | QueryInput['ExclusiveStartKey'],
  ): Promise<void> {
    let result;
    const { tableName: TableName, index: IndexName, limit: Limit } = this.input;

    if (this.query) {
      result = await queryTableIndex(TableName, IndexName, { ...this.query, Limit });
    } else {
      const query: ScanCommandInput = { TableName, IndexName, Limit, ExclusiveStartKey };
      result = await this.client.send(new ScanCommand(query));
    }
    if (result) {
      this.rows = result.Items
        ? result.Items.map((item) => {
            const cols = unmarshall(item);
            if (this.format === 'json') return cols;

            const row: { [p: string]: any } = {};
            Object.keys(cols).forEach((header) => {
              this.appendHeader(header.trim());
              const val = cols[header];
              row[header] = typeof val === 'object' ? JSON.stringify(val) : val;
            });
            return this.rowPredicate ? this.rowPredicate(row, this) : row;
          })
        : [];

      this.writeToTarget();

      if (result && result.LastEvaluatedKey) {
        await this.exec(result.LastEvaluatedKey);
      }
    }
  }
}
