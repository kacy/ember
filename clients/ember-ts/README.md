# ember-ts

TypeScript client for [ember](https://github.com/kacy/ember) — a high-performance, Redis-compatible cache built in Rust.

Communicates over gRPC (default port `6380`). Full IDE autocomplete, Buffer-based API, and async iterables for pub/sub.

## install

```bash
npm install ember-ts
```

## quickstart

```ts
import { EmberClient } from 'ember-ts';

const client = new EmberClient('localhost:6380');

await client.set('greeting', 'hello');
const val = await client.get('greeting');
console.log(val?.toString()); // "hello"

await client.set('counter', '0');
await client.incr('counter');
console.log(await client.get('counter')); // <Buffer 31> ("1")

client.close();
```

## authentication

```ts
const client = new EmberClient('localhost:6380', { password: 'secret' });
```

The password is sent as the `authorization` gRPC metadata header on every call.

## api

All methods return `Promise<T>` and throw on error.

### strings

| method | args | returns |
|--------|------|---------|
| `get(key)` | `key: string` | `Buffer \| null` |
| `set(key, value, opts?)` | `key`, `value: Buffer \| string`, [`SetOptions`](#setoptions) | `boolean` |
| `del(...keys)` | `...string` | `number` |
| `mGet(keys)` | `string[]` | `(Buffer \| null)[]` |
| `mSet(entries)` | `Record<string, Buffer \| string>` | `void` |
| `incr(key)` | `key: string` | `number` |
| `incrBy(key, delta)` | `key`, `delta: number` | `number` |
| `decrBy(key, delta)` | `key`, `delta: number` | `number` |
| `decr(key)` | `key: string` | `number` |
| `incrByFloat(key, delta)` | `key`, `delta: number` | `number` |
| `append(key, value)` | `key`, `value: Buffer \| string` | `number` |
| `strlen(key)` | `key: string` | `number` |
| `getDel(key)` | `key: string` | `Buffer \| null` |
| `getEx(key, opts?)` | `key`, [`GetExOptions`](#getexoptions) | `Buffer \| null` |
| `getRange(key, start, end)` | `key`, `start`, `end: number` | `Buffer` |
| `setRange(key, offset, value)` | `key`, `offset: number`, `value: Buffer \| string` | `number` |

### keys

| method | args | returns |
|--------|------|---------|
| `exists(...keys)` | `...string` | `number` |
| `expire(key, seconds)` | `key`, `seconds: number` | `boolean` |
| `pExpire(key, ms)` | `key`, `ms: number` | `boolean` |
| `persist(key)` | `key: string` | `boolean` |
| `ttl(key)` | `key: string` | `number` |
| `pTtl(key)` | `key: string` | `number` |
| `type(key)` | `key: string` | `string` |
| `keys(pattern)` | `pattern: string` | `string[]` |
| `rename(key, newKey)` | `key`, `newKey: string` | `void` |
| `scan(cursor, opts?)` | `cursor: number`, [`ScanOptions`](#scanoptions) | [`ScanPage`](#scanpage) |
| `copy(src, dst, replace?)` | `src`, `dst: string`, `replace?: boolean` | `boolean` |
| `randomKey()` | — | `string \| null` |
| `touch(...keys)` | `...string` | `number` |
| `unlink(...keys)` | `...string` | `number` |

### lists

| method | args | returns |
|--------|------|---------|
| `lPush(key, ...values)` | `key`, `...Buffer \| string` | `number` |
| `rPush(key, ...values)` | `key`, `...Buffer \| string` | `number` |
| `lPop(key)` | `key: string` | `Buffer \| null` |
| `rPop(key)` | `key: string` | `Buffer \| null` |
| `lRange(key, start, stop)` | `key`, `start`, `stop: number` | `Buffer[]` |
| `lLen(key)` | `key: string` | `number` |
| `lIndex(key, index)` | `key`, `index: number` | `Buffer \| null` |
| `lSet(key, index, value)` | `key`, `index: number`, `value: Buffer \| string` | `void` |
| `lTrim(key, start, stop)` | `key`, `start`, `stop: number` | `void` |
| `lInsert(key, before, pivot, value)` | `key`, `before: boolean`, `pivot`, `value: Buffer \| string` | `number` |
| `lRem(key, count, value)` | `key`, `count: number`, `value: Buffer \| string` | `number` |
| `lPos(key, value, count?)` | `key`, `value: Buffer \| string`, `count?: number` | `number \| null` |
| `lMove(src, dst, srcLeft, dstLeft)` | `src`, `dst: string`, `srcLeft`, `dstLeft: boolean` | `Buffer \| null` |

### hashes

| method | args | returns |
|--------|------|---------|
| `hSet(key, fields)` | `key`, `Record<string, Buffer \| string>` | `number` |
| `hGet(key, field)` | `key`, `field: string` | `Buffer \| null` |
| `hGetAll(key)` | `key: string` | `Record<string, Buffer>` |
| `hDel(key, ...fields)` | `key`, `...string` | `number` |
| `hExists(key, field)` | `key`, `field: string` | `boolean` |
| `hLen(key)` | `key: string` | `number` |
| `hKeys(key)` | `key: string` | `string[]` |
| `hVals(key)` | `key: string` | `Buffer[]` |
| `hmGet(key, fields)` | `key`, `string[]` | `(Buffer \| null)[]` |
| `hIncrBy(key, field, delta)` | `key`, `field: string`, `delta: number` | `number` |
| `hScan(key, cursor, opts?)` | `key`, `cursor: number`, [`ScanOptions`](#scanoptions) | [`HScanPage`](#hscanpage) |

### sets

| method | args | returns |
|--------|------|---------|
| `sAdd(key, ...members)` | `key`, `...string` | `number` |
| `sRem(key, ...members)` | `key`, `...string` | `number` |
| `sMembers(key)` | `key: string` | `string[]` |
| `sIsMember(key, member)` | `key`, `member: string` | `boolean` |
| `sCard(key)` | `key: string` | `number` |
| `sUnion(...keys)` | `...string` | `string[]` |
| `sInter(...keys)` | `...string` | `string[]` |
| `sDiff(...keys)` | `...string` | `string[]` |
| `sUnionStore(dst, ...keys)` | `dst: string`, `...string` | `number` |
| `sInterStore(dst, ...keys)` | `dst: string`, `...string` | `number` |
| `sDiffStore(dst, ...keys)` | `dst: string`, `...string` | `number` |
| `sRandMember(key, count)` | `key`, `count: number` | `string[]` |
| `sPop(key, count)` | `key`, `count: number` | `string[]` |
| `sMisMember(key, ...members)` | `key`, `...string` | `boolean[]` |
| `sScan(key, cursor, opts?)` | `key`, `cursor: number`, [`ScanOptions`](#scanoptions) | [`SScanPage`](#sscanpage) |

### sorted sets

| method | args | returns |
|--------|------|---------|
| `zAdd(key, members, opts?)` | `key`, [`ScoreMember[]`](#scoremember), [`ZAddOptions`](#zaddoptions) | `number` |
| `zRem(key, ...members)` | `key`, `...string` | `number` |
| `zScore(key, member)` | `key`, `member: string` | `number \| null` |
| `zRank(key, member)` | `key`, `member: string` | `number \| null` |
| `zRevRank(key, member)` | `key`, `member: string` | `number \| null` |
| `zCard(key)` | `key: string` | `number` |
| `zRange(key, start, stop, withScores?)` | `key`, `start`, `stop: number`, `withScores?: boolean` | [`ScoreMember[]`](#scoremember) |
| `zRevRange(key, start, stop, withScores?)` | `key`, `start`, `stop: number`, `withScores?: boolean` | [`ScoreMember[]`](#scoremember) |
| `zCount(key, min, max)` | `key`, `min`, `max: string` | `number` |
| `zIncrBy(key, delta, member)` | `key`, `delta: number`, `member: string` | `number` |
| `zRangeByScore(key, min, max, opts?)` | `key`, `min`, `max: string`, [`ZRangeByScoreOptions`](#zrangebyscoreoptions) | [`ScoreMember[]`](#scoremember) |
| `zRevRangeByScore(key, max, min, opts?)` | `key`, `max`, `min: string`, [`ZRangeByScoreOptions`](#zrangebyscoreoptions) | [`ScoreMember[]`](#scoremember) |
| `zPopMin(key, count?)` | `key`, `count?: number` | [`ScoreMember[]`](#scoremember) |
| `zPopMax(key, count?)` | `key`, `count?: number` | [`ScoreMember[]`](#scoremember) |
| `zDiff(keys, withScores?)` | `string[]`, `withScores?: boolean` | [`ScoreMember[]`](#scoremember) |
| `zInter(keys, withScores?)` | `string[]`, `withScores?: boolean` | [`ScoreMember[]`](#scoremember) |
| `zUnion(keys, withScores?)` | `string[]`, `withScores?: boolean` | [`ScoreMember[]`](#scoremember) |
| `zScan(key, cursor, opts?)` | `key`, `cursor: number`, [`ScanOptions`](#scanoptions) | [`ZScanPage`](#zscanpage) |

### vectors

Requires the server to be built with the `vector` feature.

| method | args | returns |
|--------|------|---------|
| `vAdd(key, element, vector, opts?)` | `key`, `element: string`, `number[]`, [`VAddOptions`](#vaddoptions) | `boolean` |
| `vAddBatch(key, entries, opts?)` | `key`, [`VAddBatchEntry[]`](#vaddbatchentry), [`VAddBatchOptions`](#vaddbatchoptions) | `number` |
| `vSim(key, query, count, opts?)` | `key`, `number[]`, `count: number`, [`VSimOptions`](#vsimoptions) | [`VSimResult[]`](#vsimresult) |
| `vRem(key, element)` | `key`, `element: string` | `boolean` |
| `vGet(key, element)` | `key`, `element: string` | [`VGetResult`](#vgetresult) |
| `vCard(key)` | `key: string` | `number` |
| `vDim(key)` | `key: string` | `number` |
| `vInfo(key)` | `key: string` | [`VInfoResult`](#vinforesult) |

### pub/sub

```ts
// publish
await client.publish('news', 'breaking story');

// subscribe (async iterable)
for await (const evt of client.subscribe(['news', 'alerts'])) {
  console.log(evt.kind, evt.channel, evt.data?.toString());
  if (shouldStop) break; // cancels the stream cleanly
}

// pattern subscriptions work the same way
for await (const evt of client.subscribe([], ['user:*'])) {
  console.log(`pattern match: ${evt.pattern}, channel: ${evt.channel}`);
}
```

| method | args | returns |
|--------|------|---------|
| `publish(channel, message)` | `channel: string`, `message: Buffer \| string` | `number` |
| `subscribe(channels, patterns?)` | `string[]`, `string[]` | `AsyncIterable<SubscribeEvent>` |
| `pubSubChannels(pattern?)` | `pattern?: string` | `string[]` |
| `pubSubNumSub(...channels)` | `...string` | `Map<string, number>` |
| `pubSubNumPat()` | — | `number` |

### server

| method | args | returns |
|--------|------|---------|
| `ping(message?)` | `message?: string` | `string` |
| `echo(message)` | `message: string` | `string` |
| `flushDb(async?)` | `async?: boolean` | `void` |
| `dbSize()` | — | `number` |
| `info(section?)` | `section?: string` | `string` |
| `bgSave()` | — | `string` |
| `bgRewriteAof()` | — | `string` |
| `time()` | — | [`TimeResult`](#timeresult) |
| `lastSave()` | — | `number` |

### slowlog

| method | args | returns |
|--------|------|---------|
| `slowLogGet(count?)` | `count?: number` | [`SlowLogEntry[]`](#slowlogentry) |
| `slowLogLen()` | — | `number` |
| `slowLogReset()` | — | `void` |

## types

### SetOptions

```ts
interface SetOptions {
  ex?: number;   // TTL in seconds
  px?: number;   // TTL in milliseconds
  nx?: boolean;  // only set if the key does not exist
  xx?: boolean;  // only set if the key already exists
}
```

### GetExOptions

```ts
interface GetExOptions {
  ex?: number;      // set expiry in seconds
  px?: number;      // set expiry in milliseconds
  persist?: boolean; // remove expiry, making the key permanent
}
```

### ScanOptions

```ts
interface ScanOptions {
  pattern?: string; // glob pattern to filter results
  count?: number;   // hint for page size (server may return more or fewer)
}
```

### ZAddOptions

```ts
interface ZAddOptions {
  nx?: boolean; // only add new members
  xx?: boolean; // only update existing members
  gt?: boolean; // only update if new score > current score
  lt?: boolean; // only update if new score < current score
  ch?: boolean; // count changed elements, not just added ones
}
```

### ZRangeByScoreOptions

```ts
interface ZRangeByScoreOptions {
  offset?: number;      // pagination offset
  count?: number;       // max results
  withScores?: boolean; // include scores in response
}
```

### VAddOptions

```ts
interface VAddOptions {
  metric?: VectorMetric;           // distance metric (default: cosine)
  quantization?: VectorQuantization; // storage quantization
  connectivity?: number;           // HNSW M parameter
  efConstruction?: number;         // HNSW ef_construction parameter
}
```

### VAddBatchEntry

```ts
interface VAddBatchEntry {
  element: string;
  vector: number[];
}
```

### VSimOptions

```ts
interface VSimOptions {
  efSearch?: number; // recall vs. latency trade-off
}
```

### ScoreMember

```ts
interface ScoreMember {
  member: string;
  score: number;
}
```

### VSimResult

```ts
interface VSimResult {
  element: string;
  distance: number;
}
```

### SlowLogEntry

```ts
interface SlowLogEntry {
  id: number;
  timestamp: number;       // unix timestamp
  durationMicros: number;  // execution time in microseconds
  command: string;
}
```

### SubscribeEvent

```ts
interface SubscribeEvent {
  kind: string;          // "message" or "pmessage"
  channel: string;
  data: Buffer | null;
  pattern?: string;      // set for pmessage events
}
```

### ScanPage

```ts
interface ScanPage {
  cursor: number; // 0 = scan complete
  keys: string[];
}
```

### HScanPage

```ts
interface HScanPage {
  cursor: number;
  fields: Record<string, Buffer>;
}
```

### ZScanPage

```ts
interface ZScanPage {
  cursor: number;
  members: ScoreMember[];
}
```

### SScanPage

```ts
interface SScanPage {
  cursor: number;
  members: string[];
}
```

### TimeResult

```ts
interface TimeResult {
  seconds: number;      // unix timestamp (whole seconds)
  microseconds: number; // offset within the current second
}
```

### VGetResult

```ts
interface VGetResult {
  exists: boolean;
  vector: number[];
}
```

### VInfoResult

```ts
interface VInfoResult {
  exists: boolean;
  info: Record<string, string>; // metric, dimensions, capacity, etc.
}
```

## scan example

```ts
let cursor = 0;
do {
  const page = await client.scan(cursor, { pattern: 'user:*', count: 100 });
  cursor = page.cursor;
  for (const key of page.keys) {
    console.log(key);
  }
} while (cursor !== 0);
```

## leaderboard example

```ts
// add scores
await client.zAdd('scores', [
  { member: 'alice', score: 9500 },
  { member: 'bob',   score: 8200 },
  { member: 'carol', score: 9800 },
]);

// top 3
const top = await client.zRange('scores', 0, 2, true);
top.forEach(({ member, score }) => console.log(member, score));

// carol's rank (0-indexed from highest)
const rank = await client.zRevRank('scores', 'carol'); // 0
```

## connection details

- default address: `localhost:6380`
- transport: gRPC over plaintext (TLS support coming)
- the proto file is bundled in `proto/ember/v1/ember.proto`
