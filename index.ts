import { DynamoDBClient, QueryInput, ScanCommand, ScanCommandInput } from '@aws-sdk/client-dynamodb';
import * as fs from 'fs';
import { FilterExpressionMap, KeyCondExpressionMap, queryTableIndex } from 'dynadash';
import csv from 'papaparse';
import { WriteStream } from 'fs';
import { unmarshall } from '@aws-sdk/util-dynamodb';

const DEFAULT_QUERY_LIMIT = 2000;

type Params = {
  client: DynamoDBClient;
  input: {
    tableName: ScanCommandInput['TableName'];
    index?: ScanCommandInput['IndexName'];
    limit?: ScanCommandInput['Limit'];
  };
  query?: {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  };
  target: string | WriteStream;
};

export default class Dynamocsv {
  private readonly input: Params['input'];
  private readonly query: Params['query'];
  private client: DynamoDBClient;
  private writeCount: number;
  private readonly targetStream: WriteStream;
  private readonly headers: Set<string>;
  private rows: any[];

  constructor({ client, target, input, query }: Params) {
    this.client = client;
    this.input = { ...input, limit: input.limit || DEFAULT_QUERY_LIMIT };
    this.query = query;
    this.writeCount = 0;
    this.headers = new Set();
    this.rows = [];
    this.targetStream = target instanceof WriteStream ? target : fs.createWriteStream(target, { flags: 'a' });
  }

  private writeToTarget(): void {
    let endData = csv.unparse({
      fields: [...this.headers],
      data: this.rows,
    });

    if (this.writeCount > 0) {
      // remove column names after first write chunk.
      endData = endData.replace(/(.*\r\n)/, '');
    }

    if (this.targetStream) {
      this.targetStream.write(endData);
    }
    // reset write array. saves memory
    this.writeCount += this.rows.length;
    this.rows = [];
  }

  private addHeader(header: string): void {
    if (!this.headers.has(header)) this.headers.add(header);
  }

  /**
   * @param ExclusiveStartKey
   */
  async queryDb(
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
            const row: { [p: string]: any } = {};
            const data = unmarshall(item);
            Object.keys(data).forEach((key) => {
              this.addHeader(key.trim());
              const val = data[key];
              row[key] = typeof val === 'object' ? JSON.stringify(val) : val;
            });
            return row;
          })
        : [];

      this.writeToTarget();

      if (result && result.LastEvaluatedKey) {
        await this.queryDb(result.LastEvaluatedKey);
      }
    }
  }
}
