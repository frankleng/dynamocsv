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
  target: string | WriteStream;
  query?: {
    keyCondExpressionMap?: KeyCondExpressionMap;
    filterExpressionMap?: FilterExpressionMap;
  };
  rowPredicate?: (data: any, context: Dynamocsv) => any;
};

export default class Dynamocsv {
  private readonly input: Params['input'];
  private readonly query: Params['query'];
  private readonly targetStream: WriteStream;
  private readonly rowPredicate: ((data: any, context: Dynamocsv) => any) | undefined;
  private client: DynamoDBClient;
  private writeCount: number;
  private headers: Set<string>;
  private rows: any[];

  constructor({ client, target, input, query, rowPredicate }: Params) {
    this.client = client;
    this.input = { ...input, limit: input.limit || DEFAULT_QUERY_LIMIT };
    this.query = query;
    this.writeCount = 0;
    this.headers = new Set();
    this.rows = [];
    this.targetStream = target instanceof WriteStream ? target : fs.createWriteStream(target, { flags: 'a' });
    this.rowPredicate = rowPredicate;
  }

  private writeToTarget(): void {
    let endData = csv.unparse({
      fields: [...this.headers.values()],
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
            const row: { [p: string]: any } = {};
            const cols = unmarshall(item);
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
