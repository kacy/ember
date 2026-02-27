import * as grpc from '@grpc/grpc-js';
import * as protoLoader from '@grpc/proto-loader';
import path from 'path';

import type {
  ClientOptions,
  SetOptions,
  GetExOptions,
  ScanOptions,
  ZAddOptions,
  ZRangeByScoreOptions,
  VAddOptions,
  VAddBatchOptions,
  VAddBatchEntry,
  VSimOptions,
  ScoreMember,
  VSimResult,
  SlowLogEntry,
  SubscribeEvent,
  ScanPage,
  HScanPage,
  ZScanPage,
  SScanPage,
  TimeResult,
  VGetResult,
  VInfoResult,
} from './types';

// ---------------------------------------------------------------------------
// proto loading — done once, cached for the lifetime of the process
// ---------------------------------------------------------------------------

let _stubCtor: grpc.ServiceClientConstructor | undefined;

function loadStub(): grpc.ServiceClientConstructor {
  if (_stubCtor) return _stubCtor;

  const protoPath = path.join(__dirname, '../proto/ember/v1/ember.proto');
  const pkgDef = protoLoader.loadSync(protoPath, {
    keepCase: false,
    longs: Number,
    enums: String,
    defaults: true,
    oneofs: true,
  });

  const pkg = grpc.loadPackageDefinition(pkgDef) as any;
  _stubCtor = pkg.ember.v1.EmberCache as grpc.ServiceClientConstructor;
  return _stubCtor;
}

// ---------------------------------------------------------------------------
// client
// ---------------------------------------------------------------------------

/**
 * A gRPC client for the ember cache server.
 *
 * All methods return Promises and throw on error. Pub/sub streaming is
 * exposed as an `AsyncIterable` so you can use `for await` naturally.
 *
 * @example
 * ```ts
 * const client = new EmberClient('localhost:6380');
 * await client.set('greeting', 'hello');
 * const val = await client.get('greeting');
 * console.log(val?.toString()); // "hello"
 * client.close();
 * ```
 */
export class EmberClient {
  private readonly stub: InstanceType<grpc.ServiceClientConstructor>;
  private readonly meta: grpc.Metadata;

  /**
   * @param address - `host:port` of the ember server. defaults to `localhost:6380`.
   * @param options - optional client configuration.
   */
  constructor(address = 'localhost:6380', options: ClientOptions = {}) {
    const Ctor = loadStub();
    this.stub = new Ctor(address, grpc.credentials.createInsecure());
    this.meta = new grpc.Metadata();
    if (options.password) {
      this.meta.set('authorization', options.password);
    }
  }

  /** closes the underlying gRPC connection. */
  close(): void {
    this.stub.close();
  }

  // wraps a unary gRPC call in a Promise
  private call<T>(method: string, req: object): Promise<T> {
    return new Promise((resolve, reject) => {
      (this.stub as any)[method](req, this.meta, (err: Error | null, res: T) => {
        if (err) reject(err);
        else resolve(res);
      });
    });
  }

  // wraps a server-streaming RPC as an AsyncIterable
  private stream<T>(method: string, req: object): AsyncIterable<T> {
    return {
      [Symbol.asyncIterator]: () => {
        const call = (this.stub as any)[method](req, this.meta) as grpc.ClientReadableStream<T>;

        // buffer events that arrive before the next() caller is ready
        const queue: Array<{ value?: T; error?: Error; done?: true }> = [];
        let waiter: {
          resolve: (v: IteratorResult<T>) => void;
          reject: (e: Error) => void;
        } | null = null;

        const enqueue = (item: { value?: T; error?: Error; done?: true }) => {
          if (waiter) {
            const w = waiter;
            waiter = null;
            if (item.error) w.reject(item.error);
            else w.resolve({ value: item.value as T, done: !!item.done });
          } else {
            queue.push(item);
          }
        };

        call.on('data', (v: T) => enqueue({ value: v }));
        call.on('end', () => enqueue({ done: true }));
        call.on('error', (err: Error) => enqueue({ error: err }));

        return {
          next(): Promise<IteratorResult<T>> {
            if (queue.length > 0) {
              const item = queue.shift()!;
              if (item.error) return Promise.reject(item.error);
              return Promise.resolve({ value: item.value as T, done: !!item.done });
            }
            return new Promise<IteratorResult<T>>((resolve, reject) => {
              waiter = { resolve, reject };
            });
          },
          return(): Promise<IteratorResult<T>> {
            call.cancel();
            return Promise.resolve({ value: undefined as unknown as T, done: true });
          },
        };
      },
    };
  }

  // ---------------------------------------------------------------------------
  // strings
  // ---------------------------------------------------------------------------

  /**
   * Returns the value for a key, or `null` if the key does not exist.
   */
  async get(key: string): Promise<Buffer | null> {
    const res = await this.call<{ value?: Buffer }>('get', { key });
    return res.value ?? null;
  }

  /**
   * Stores a key-value pair. Returns `true` if the key was set.
   *
   * NX/XX conditions: if the condition is not met, returns `false` without
   * modifying the key.
   *
   * @example
   * ```ts
   * await client.set('counter', '0', { ex: 60 }); // expires in 60 seconds
   * await client.set('lock', '1', { nx: true });   // only if absent
   * ```
   */
  async set(key: string, value: Buffer | string, opts: SetOptions = {}): Promise<boolean> {
    const req: Record<string, unknown> = {
      key,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    };
    if (opts.ex) req.expireSeconds = opts.ex;
    if (opts.px) req.expireMillis = opts.px;
    if (opts.nx) req.nx = true;
    if (opts.xx) req.xx = true;
    const res = await this.call<{ ok: boolean }>('set', req);
    return res.ok;
  }

  /**
   * Removes one or more keys. Returns the number of keys deleted.
   */
  async del(...keys: string[]): Promise<number> {
    const res = await this.call<{ deleted: number }>('del', { keys });
    return res.deleted;
  }

  /**
   * Returns values for multiple keys in one round-trip.
   * Missing keys have `null` at their position.
   */
  async mGet(keys: string[]): Promise<(Buffer | null)[]> {
    const res = await this.call<{ values: Array<{ value?: Buffer }> }>('mGet', { keys });
    return res.values.map(v => v.value ?? null);
  }

  /**
   * Sets multiple key-value pairs atomically.
   */
  async mSet(entries: Record<string, Buffer | string>): Promise<void> {
    const pairs = Object.entries(entries).map(([key, value]) => ({
      key,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    }));
    await this.call('mSet', { pairs });
  }

  /**
   * Increments a key by 1 and returns the new value.
   * Creates the key with value `0` before incrementing if it does not exist.
   */
  async incr(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('incr', { key });
    return res.value;
  }

  /**
   * Increments a key by `delta` and returns the new value.
   */
  async incrBy(key: string, delta: number): Promise<number> {
    const res = await this.call<{ value: number }>('incrBy', { key, delta });
    return res.value;
  }

  /**
   * Decrements a key by `delta` and returns the new value.
   */
  async decrBy(key: string, delta: number): Promise<number> {
    const res = await this.call<{ value: number }>('decrBy', { key, delta });
    return res.value;
  }

  /**
   * Decrements a key by 1 and returns the new value.
   */
  async decr(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('decr', { key });
    return res.value;
  }

  /**
   * Increments a key by a floating-point `delta` and returns the new value.
   */
  async incrByFloat(key: string, delta: number): Promise<number> {
    const res = await this.call<{ value: string }>('incrByFloat', { key, delta });
    return parseFloat(res.value);
  }

  /**
   * Appends `value` to the string at `key`. Returns the new string length.
   */
  async append(key: string, value: Buffer | string): Promise<number> {
    const res = await this.call<{ value: number }>('append', {
      key,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    });
    return res.value;
  }

  /**
   * Returns the byte length of the string at `key`. Returns `0` if the key does not exist.
   */
  async strlen(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('strlen', { key });
    return res.value;
  }

  /**
   * Atomically gets and deletes a key. Returns `null` if the key did not exist.
   */
  async getDel(key: string): Promise<Buffer | null> {
    const res = await this.call<{ value?: Buffer }>('getDel', { key });
    return res.value ?? null;
  }

  /**
   * Gets a key and optionally updates its expiry in the same round-trip.
   * Pass `{ persist: true }` to remove the expiry.
   */
  async getEx(key: string, opts: GetExOptions = {}): Promise<Buffer | null> {
    const req: Record<string, unknown> = { key };
    if (opts.ex) req.expireSeconds = opts.ex;
    if (opts.px) req.expireMillis = opts.px;
    if (opts.persist) req.persist = true;
    const res = await this.call<{ value?: Buffer }>('getEx', req);
    return res.value ?? null;
  }

  /**
   * Returns the substring of the string at `key` for the byte range [start, end].
   * Negative indices count from the end of the string.
   */
  async getRange(key: string, start: number, end: number): Promise<Buffer> {
    const res = await this.call<{ value?: Buffer }>('getRange', { key, start, end });
    return res.value ?? Buffer.alloc(0);
  }

  /**
   * Overwrites part of the string at `key` starting at byte `offset`.
   * Returns the new string length.
   */
  async setRange(key: string, offset: number, value: Buffer | string): Promise<number> {
    const res = await this.call<{ value: number }>('setRange', {
      key,
      offset,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    });
    return res.value;
  }

  // ---------------------------------------------------------------------------
  // keys
  // ---------------------------------------------------------------------------

  /**
   * Returns the number of the given keys that exist.
   * A key specified multiple times is counted multiple times.
   */
  async exists(...keys: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('exists', { keys });
    return res.value;
  }

  /**
   * Sets a timeout on a key in seconds. Returns `true` if the timeout was set.
   */
  async expire(key: string, seconds: number): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('expire', { key, seconds });
    return res.value;
  }

  /**
   * Sets a timeout on a key in milliseconds. Returns `true` if the timeout was set.
   */
  async pExpire(key: string, milliseconds: number): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('pExpire', { key, milliseconds });
    return res.value;
  }

  /**
   * Removes the expiry from a key, making it persistent.
   * Returns `true` if the timeout was removed, `false` if the key has no expiry.
   */
  async persist(key: string): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('persist', { key });
    return res.value;
  }

  /**
   * Returns the remaining time to live in seconds.
   * Returns `-1` if the key has no expiry, `-2` if the key does not exist.
   */
  async ttl(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('ttl', { key });
    return res.value;
  }

  /**
   * Returns the remaining time to live in milliseconds.
   * Returns `-1` if the key has no expiry, `-2` if the key does not exist.
   */
  async pTtl(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('pTtl', { key });
    return res.value;
  }

  /**
   * Returns the data type stored at `key`: `"string"`, `"list"`, `"set"`,
   * `"zset"`, `"hash"`, or `"none"` if the key does not exist.
   */
  async type(key: string): Promise<string> {
    const res = await this.call<{ typeName: string }>('type', { key });
    return res.typeName;
  }

  /**
   * Returns all keys matching `pattern`. Supports glob-style patterns:
   * `*` matches any sequence, `?` matches any single character, `[abc]` matches a character set.
   *
   * Avoid running KEYS in production on large datasets — use SCAN instead.
   */
  async keys(pattern: string): Promise<string[]> {
    const res = await this.call<{ keys: string[] }>('keys', { pattern });
    return res.keys;
  }

  /**
   * Renames `key` to `newKey`. Throws if `key` does not exist.
   */
  async rename(key: string, newKey: string): Promise<void> {
    await this.call('rename', { key, newKey });
  }

  /**
   * Iterates over the keyspace one page at a time.
   * Start with cursor `0`; the scan is complete when the returned cursor is `0`.
   *
   * @example
   * ```ts
   * let cursor = 0;
   * do {
   *   const page = await client.scan(cursor, { pattern: 'user:*', count: 100 });
   *   cursor = page.cursor;
   *   for (const key of page.keys) console.log(key);
   * } while (cursor !== 0);
   * ```
   */
  async scan(cursor: number, opts: ScanOptions = {}): Promise<ScanPage> {
    const req: Record<string, unknown> = { cursor, count: opts.count ?? 0 };
    if (opts.pattern != null) req.pattern = opts.pattern;
    const res = await this.call<{ cursor: number; keys: string[] }>('scan', req);
    return { cursor: res.cursor, keys: res.keys };
  }

  /**
   * Copies `source` to `destination`. Pass `replace: true` to overwrite an existing key.
   * Returns `true` if the key was copied.
   */
  async copy(source: string, destination: string, replace = false): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('copy', { source, destination, replace });
    return res.value;
  }

  /**
   * Returns a random key from the keyspace, or `null` if the database is empty.
   */
  async randomKey(): Promise<string | null> {
    const res = await this.call<{ value?: Buffer }>('randomKey', {});
    return res.value ? res.value.toString() : null;
  }

  /**
   * Updates the last-access time for the given keys without changing their values.
   * Returns the number of keys that exist.
   */
  async touch(...keys: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('touch', { keys });
    return res.value;
  }

  /**
   * Removes keys asynchronously (background deallocation).
   * Returns the number of keys removed.
   */
  async unlink(...keys: string[]): Promise<number> {
    const res = await this.call<{ deleted: number }>('unlink', { keys });
    return res.deleted;
  }

  // ---------------------------------------------------------------------------
  // lists
  // ---------------------------------------------------------------------------

  /**
   * Prepends one or more values to a list. Returns the new list length.
   */
  async lPush(key: string, ...values: (Buffer | string)[]): Promise<number> {
    const res = await this.call<{ value: number }>('lPush', {
      key,
      values: values.map(v => (Buffer.isBuffer(v) ? v : Buffer.from(v))),
    });
    return res.value;
  }

  /**
   * Appends one or more values to a list. Returns the new list length.
   */
  async rPush(key: string, ...values: (Buffer | string)[]): Promise<number> {
    const res = await this.call<{ value: number }>('rPush', {
      key,
      values: values.map(v => (Buffer.isBuffer(v) ? v : Buffer.from(v))),
    });
    return res.value;
  }

  /**
   * Removes and returns the first element of a list.
   * Returns `null` if the list is empty or does not exist.
   */
  async lPop(key: string): Promise<Buffer | null> {
    const res = await this.call<{ value?: Buffer }>('lPop', { key });
    return res.value ?? null;
  }

  /**
   * Removes and returns the last element of a list.
   * Returns `null` if the list is empty or does not exist.
   */
  async rPop(key: string): Promise<Buffer | null> {
    const res = await this.call<{ value?: Buffer }>('rPop', { key });
    return res.value ?? null;
  }

  /**
   * Returns the elements in the range [start, stop].
   * Negative indices count from the tail: `-1` is the last element.
   */
  async lRange(key: string, start: number, stop: number): Promise<Buffer[]> {
    const res = await this.call<{ values: Buffer[] }>('lRange', { key, start, stop });
    return res.values;
  }

  /**
   * Returns the number of elements in a list.
   */
  async lLen(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('lLen', { key });
    return res.value;
  }

  /**
   * Returns the element at `index`. Negative indices count from the tail.
   * Returns `null` if the index is out of range.
   */
  async lIndex(key: string, index: number): Promise<Buffer | null> {
    const res = await this.call<{ value?: Buffer }>('lIndex', { key, index });
    return res.value ?? null;
  }

  /**
   * Sets the element at `index` to `value`. Throws if the index is out of range.
   */
  async lSet(key: string, index: number, value: Buffer | string): Promise<void> {
    await this.call('lSet', {
      key,
      index,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    });
  }

  /**
   * Trims the list so it only contains elements in [start, stop].
   */
  async lTrim(key: string, start: number, stop: number): Promise<void> {
    await this.call('lTrim', { key, start, stop });
  }

  /**
   * Inserts `value` before or after the first occurrence of `pivot` in the list.
   * Returns the new list length, or `-1` if `pivot` was not found.
   */
  async lInsert(
    key: string,
    before: boolean,
    pivot: Buffer | string,
    value: Buffer | string,
  ): Promise<number> {
    const res = await this.call<{ value: number }>('lInsert', {
      key,
      before,
      pivot: Buffer.isBuffer(pivot) ? pivot : Buffer.from(pivot),
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    });
    return res.value;
  }

  /**
   * Removes occurrences of `value` from the list.
   * `count > 0`: remove from head; `count < 0`: remove from tail; `0`: remove all.
   * Returns the number of elements removed.
   */
  async lRem(key: string, count: number, value: Buffer | string): Promise<number> {
    const res = await this.call<{ value: number }>('lRem', {
      key,
      count,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    });
    return res.value;
  }

  /**
   * Returns the first index of `value` in the list, or `null` if not found.
   * Pass `count` to find the first N occurrences (returns the first match only when absent).
   */
  async lPos(key: string, value: Buffer | string, count?: number): Promise<number | null> {
    const req: Record<string, unknown> = {
      key,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    };
    if (count != null) req.count = count;
    const res = await this.call<{ value?: number }>('lPos', req);
    return res.value ?? null;
  }

  /**
   * Atomically pops an element from `source` and pushes it to `destination`.
   * Pass `srcLeft: true` to pop from the head of source,
   * `dstLeft: true` to push to the head of destination.
   * Returns the moved element, or `null` if `source` is empty.
   */
  async lMove(
    source: string,
    destination: string,
    srcLeft: boolean,
    dstLeft: boolean,
  ): Promise<Buffer | null> {
    const res = await this.call<{ value?: Buffer }>('lMove', {
      source,
      destination,
      srcLeft,
      dstLeft,
    });
    return res.value ?? null;
  }

  // ---------------------------------------------------------------------------
  // hashes
  // ---------------------------------------------------------------------------

  /**
   * Sets one or more fields in a hash. Returns the number of new fields added.
   *
   * @example
   * ```ts
   * await client.hSet('user:1', { name: 'alice', email: 'alice@example.com' });
   * ```
   */
  async hSet(key: string, fields: Record<string, Buffer | string>): Promise<number> {
    const fieldValues = Object.entries(fields).map(([field, value]) => ({
      field,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    }));
    const res = await this.call<{ value: number }>('hSet', { key, fields: fieldValues });
    return res.value;
  }

  /**
   * Returns the value of a field in a hash, or `null` if the field does not exist.
   */
  async hGet(key: string, field: string): Promise<Buffer | null> {
    const res = await this.call<{ value?: Buffer }>('hGet', { key, field });
    return res.value ?? null;
  }

  /**
   * Returns all fields and values in a hash as a plain object.
   */
  async hGetAll(key: string): Promise<Record<string, Buffer>> {
    const res = await this.call<{ fields: Array<{ field: string; value: Buffer }> }>(
      'hGetAll',
      { key },
    );
    const result: Record<string, Buffer> = {};
    for (const fv of res.fields) {
      result[fv.field] = fv.value;
    }
    return result;
  }

  /**
   * Removes fields from a hash. Returns the number of fields deleted.
   */
  async hDel(key: string, ...fields: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('hDel', { key, fields });
    return res.value;
  }

  /**
   * Returns `true` if a field exists in a hash.
   */
  async hExists(key: string, field: string): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('hExists', { key, field });
    return res.value;
  }

  /**
   * Returns the number of fields in a hash.
   */
  async hLen(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('hLen', { key });
    return res.value;
  }

  /**
   * Returns all field names in a hash.
   */
  async hKeys(key: string): Promise<string[]> {
    const res = await this.call<{ keys: string[] }>('hKeys', { key });
    return res.keys;
  }

  /**
   * Returns all field values in a hash.
   */
  async hVals(key: string): Promise<Buffer[]> {
    const res = await this.call<{ values: Buffer[] }>('hVals', { key });
    return res.values;
  }

  /**
   * Returns values for the specified fields in a hash.
   * Missing fields have `null` at their position.
   */
  async hmGet(key: string, fields: string[]): Promise<(Buffer | null)[]> {
    const res = await this.call<{ values: Array<{ value?: Buffer }> }>('hmGet', { key, fields });
    return res.values.map(v => v.value ?? null);
  }

  /**
   * Increments the integer value of a hash field by `delta`. Returns the new value.
   */
  async hIncrBy(key: string, field: string, delta: number): Promise<number> {
    const res = await this.call<{ value: number }>('hIncrBy', { key, field, delta });
    return res.value;
  }

  /**
   * Iterates over fields in a hash. Start with cursor `0`; done when the
   * returned cursor is `0`.
   */
  async hScan(key: string, cursor: number, opts: ScanOptions = {}): Promise<HScanPage> {
    const req: Record<string, unknown> = { key, cursor, count: opts.count ?? 0 };
    if (opts.pattern != null) req.pattern = opts.pattern;
    const res = await this.call<{
      cursor: number;
      fields: Array<{ field: string; value: Buffer }>;
    }>('hScan', req);
    const fields: Record<string, Buffer> = {};
    for (const fv of res.fields) {
      fields[fv.field] = fv.value;
    }
    return { cursor: res.cursor, fields };
  }

  // ---------------------------------------------------------------------------
  // sets
  // ---------------------------------------------------------------------------

  /**
   * Adds members to a set. Returns the number of new members added.
   */
  async sAdd(key: string, ...members: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('sAdd', { key, members });
    return res.value;
  }

  /**
   * Removes members from a set. Returns the number of members removed.
   */
  async sRem(key: string, ...members: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('sRem', { key, members });
    return res.value;
  }

  /**
   * Returns all members of a set.
   */
  async sMembers(key: string): Promise<string[]> {
    const res = await this.call<{ keys: string[] }>('sMembers', { key });
    return res.keys;
  }

  /**
   * Returns `true` if `member` belongs to the set at `key`.
   */
  async sIsMember(key: string, member: string): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('sIsMember', { key, member });
    return res.value;
  }

  /**
   * Returns the number of members in a set.
   */
  async sCard(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('sCard', { key });
    return res.value;
  }

  /**
   * Returns the union of two or more sets.
   */
  async sUnion(...keys: string[]): Promise<string[]> {
    const res = await this.call<{ keys: string[] }>('sUnion', { keys });
    return res.keys;
  }

  /**
   * Returns the intersection of two or more sets.
   */
  async sInter(...keys: string[]): Promise<string[]> {
    const res = await this.call<{ keys: string[] }>('sInter', { keys });
    return res.keys;
  }

  /**
   * Returns the members in the first set that are not in any of the subsequent sets.
   */
  async sDiff(...keys: string[]): Promise<string[]> {
    const res = await this.call<{ keys: string[] }>('sDiff', { keys });
    return res.keys;
  }

  /**
   * Stores the union of sets at `destination`. Returns the number of elements stored.
   */
  async sUnionStore(destination: string, ...keys: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('sUnionStore', { destination, keys });
    return res.value;
  }

  /**
   * Stores the intersection of sets at `destination`. Returns the number of elements stored.
   */
  async sInterStore(destination: string, ...keys: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('sInterStore', { destination, keys });
    return res.value;
  }

  /**
   * Stores the difference of sets at `destination`. Returns the number of elements stored.
   */
  async sDiffStore(destination: string, ...keys: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('sDiffStore', { destination, keys });
    return res.value;
  }

  /**
   * Returns `count` random members from a set.
   * `count > 0` returns unique members; `count < 0` allows duplicates.
   */
  async sRandMember(key: string, count: number): Promise<string[]> {
    const res = await this.call<{ values: Buffer[] }>('sRandMember', { key, count });
    return res.values.map(v => v.toString());
  }

  /**
   * Removes and returns `count` random members from a set.
   */
  async sPop(key: string, count: number): Promise<string[]> {
    const res = await this.call<{ values: Buffer[] }>('sPop', { key, count });
    return res.values.map(v => v.toString());
  }

  /**
   * Returns a boolean for each member indicating whether it belongs to the set.
   */
  async sMisMember(key: string, ...members: string[]): Promise<boolean[]> {
    const res = await this.call<{ values: boolean[] }>('sMisMember', { key, members });
    return res.values;
  }

  /**
   * Iterates over members of a set. Start with cursor `0`; done when the
   * returned cursor is `0`.
   */
  async sScan(key: string, cursor: number, opts: ScanOptions = {}): Promise<SScanPage> {
    const req: Record<string, unknown> = { key, cursor, count: opts.count ?? 0 };
    if (opts.pattern != null) req.pattern = opts.pattern;
    const res = await this.call<{ cursor: number; members: string[] }>('sScan', req);
    return { cursor: res.cursor, members: res.members };
  }

  // ---------------------------------------------------------------------------
  // sorted sets
  // ---------------------------------------------------------------------------

  /**
   * Adds members to a sorted set. Returns the number added
   * (or changed when `ch: true`).
   *
   * @example
   * ```ts
   * await client.zAdd('leaderboard', [
   *   { member: 'alice', score: 9500 },
   *   { member: 'bob',   score: 8200 },
   * ]);
   * ```
   */
  async zAdd(key: string, members: ScoreMember[], opts: ZAddOptions = {}): Promise<number> {
    const req: Record<string, unknown> = {
      key,
      members: members.map(m => ({ score: m.score, member: m.member })),
    };
    if (opts.nx) req.nx = true;
    if (opts.xx) req.xx = true;
    if (opts.gt) req.gt = true;
    if (opts.lt) req.lt = true;
    if (opts.ch) req.ch = true;
    const res = await this.call<{ value: number }>('zAdd', req);
    return res.value;
  }

  /**
   * Removes members from a sorted set. Returns the number removed.
   */
  async zRem(key: string, ...members: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('zRem', { key, members });
    return res.value;
  }

  /**
   * Returns the score of a member, or `null` if the member does not exist.
   */
  async zScore(key: string, member: string): Promise<number | null> {
    const res = await this.call<{ value?: number }>('zScore', { key, member });
    return res.value ?? null;
  }

  /**
   * Returns the 0-based rank of a member (sorted ascending by score),
   * or `null` if the member does not exist.
   */
  async zRank(key: string, member: string): Promise<number | null> {
    const res = await this.call<{ value?: number }>('zRank', { key, member });
    return res.value ?? null;
  }

  /**
   * Returns the reverse rank of a member (rank 0 = highest score),
   * or `null` if the member does not exist.
   */
  async zRevRank(key: string, member: string): Promise<number | null> {
    const res = await this.call<{ value?: number }>('zRevRank', { key, member });
    return res.value ?? null;
  }

  /**
   * Returns the number of members in a sorted set.
   */
  async zCard(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('zCard', { key });
    return res.value;
  }

  /**
   * Returns members by rank in ascending order. Pass `withScores: true` to
   * include scores (always populated in the result regardless, but only
   * meaningful when requested).
   */
  async zRange(key: string, start: number, stop: number, withScores = false): Promise<ScoreMember[]> {
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>('zRange', {
      key, start, stop, withScores,
    });
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Returns members by rank in descending order.
   */
  async zRevRange(key: string, start: number, stop: number, withScores = false): Promise<ScoreMember[]> {
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>('zRevRange', {
      key, start, stop, withScores,
    });
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Returns the count of members with scores in the range [min, max].
   * Supports redis score range syntax: `"-inf"`, `"+inf"`, `"(5"` (exclusive), `"5"`.
   */
  async zCount(key: string, min: string, max: string): Promise<number> {
    const res = await this.call<{ value: number }>('zCount', { key, min, max });
    return res.value;
  }

  /**
   * Increments the score of `member` by `delta`. Returns the new score.
   */
  async zIncrBy(key: string, delta: number, member: string): Promise<number> {
    const res = await this.call<{ value: string }>('zIncrBy', { key, delta, member });
    return parseFloat(res.value);
  }

  /**
   * Returns members with scores in [min, max] in ascending order.
   * Supports redis score range syntax.
   */
  async zRangeByScore(
    key: string,
    min: string,
    max: string,
    opts: ZRangeByScoreOptions = {},
  ): Promise<ScoreMember[]> {
    const req: Record<string, unknown> = { key, min, max, withScores: opts.withScores ?? false };
    if (opts.offset != null) req.offset = opts.offset;
    if (opts.count != null) req.count = opts.count;
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>(
      'zRangeByScore', req,
    );
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Returns members with scores in [min, max] in descending order.
   * Note: `max` comes before `min` for reverse range queries.
   */
  async zRevRangeByScore(
    key: string,
    max: string,
    min: string,
    opts: ZRangeByScoreOptions = {},
  ): Promise<ScoreMember[]> {
    const req: Record<string, unknown> = { key, max, min, withScores: opts.withScores ?? false };
    if (opts.offset != null) req.offset = opts.offset;
    if (opts.count != null) req.count = opts.count;
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>(
      'zRevRangeByScore', req,
    );
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Removes and returns up to `count` members with the lowest scores.
   */
  async zPopMin(key: string, count = 1): Promise<ScoreMember[]> {
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>(
      'zPopMin', { key, count },
    );
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Removes and returns up to `count` members with the highest scores.
   */
  async zPopMax(key: string, count = 1): Promise<ScoreMember[]> {
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>(
      'zPopMax', { key, count },
    );
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Returns members that appear in the first key but not in any subsequent key.
   */
  async zDiff(keys: string[], withScores = false): Promise<ScoreMember[]> {
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>(
      'zDiff', { keys, withScores },
    );
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Returns members that appear in all of the given sorted sets.
   */
  async zInter(keys: string[], withScores = false): Promise<ScoreMember[]> {
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>(
      'zInter', { keys, withScores },
    );
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Returns the union of all given sorted sets.
   */
  async zUnion(keys: string[], withScores = false): Promise<ScoreMember[]> {
    const res = await this.call<{ members: Array<{ score: number; member: string }> }>(
      'zUnion', { keys, withScores },
    );
    return res.members.map(m => ({ member: m.member, score: m.score }));
  }

  /**
   * Iterates over members of a sorted set. Start with cursor `0`; done when
   * the returned cursor is `0`.
   */
  async zScan(key: string, cursor: number, opts: ScanOptions = {}): Promise<ZScanPage> {
    const req: Record<string, unknown> = { key, cursor, count: opts.count ?? 0 };
    if (opts.pattern != null) req.pattern = opts.pattern;
    const res = await this.call<{
      cursor: number;
      members: Array<{ score: number; member: string }>;
    }>('zScan', req);
    return {
      cursor: res.cursor,
      members: res.members.map(m => ({ member: m.member, score: m.score })),
    };
  }

  // ---------------------------------------------------------------------------
  // vectors (requires server built with the `vector` feature)
  // ---------------------------------------------------------------------------

  /**
   * Adds a vector to a vector set. Returns `true` if the element was newly added.
   *
   * @example
   * ```ts
   * await client.vAdd('embeddings', 'doc:1', [0.1, 0.2, 0.3]);
   * ```
   */
  async vAdd(
    key: string,
    element: string,
    vector: number[],
    opts: VAddOptions = {},
  ): Promise<boolean> {
    const req: Record<string, unknown> = { key, element, vector };
    if (opts.metric) req.metric = opts.metric;
    if (opts.quantization) req.quantization = opts.quantization;
    if (opts.connectivity != null) req.connectivity = opts.connectivity;
    if (opts.efConstruction != null) req.efConstruction = opts.efConstruction;
    const res = await this.call<{ value: boolean }>('vAdd', req);
    return res.value;
  }

  /**
   * Adds multiple vectors to a vector set in one call.
   * Returns the number of elements added.
   */
  async vAddBatch(
    key: string,
    entries: VAddBatchEntry[],
    opts: VAddBatchOptions = {},
  ): Promise<number> {
    const req: Record<string, unknown> = { key, entries };
    if (opts.metric) req.metric = opts.metric;
    if (opts.quantization) req.quantization = opts.quantization;
    if (opts.connectivity != null) req.connectivity = opts.connectivity;
    if (opts.efConstruction != null) req.efConstruction = opts.efConstruction;
    const res = await this.call<{ value: number }>('vAddBatch', req);
    return res.value;
  }

  /**
   * Searches for the `count` nearest neighbors to `query` in the vector set.
   * Results are returned sorted by similarity (closest first).
   */
  async vSim(
    key: string,
    query: number[],
    count: number,
    opts: VSimOptions = {},
  ): Promise<VSimResult[]> {
    const req: Record<string, unknown> = { key, query, count };
    if (opts.efSearch != null) req.efSearch = opts.efSearch;
    const res = await this.call<{ results: Array<{ element: string; distance: number }> }>(
      'vSim', req,
    );
    return res.results.map(r => ({ element: r.element, distance: r.distance }));
  }

  /**
   * Removes an element from a vector set. Returns `true` if the element was removed.
   */
  async vRem(key: string, element: string): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('vRem', { key, element });
    return res.value;
  }

  /**
   * Returns the stored vector for `element`.
   * `exists: false` if the element is not in the set.
   */
  async vGet(key: string, element: string): Promise<VGetResult> {
    const res = await this.call<{ exists?: boolean; vector: number[] }>('vGet', { key, element });
    return { exists: res.exists ?? false, vector: res.vector ?? [] };
  }

  /**
   * Returns the number of elements in a vector set.
   */
  async vCard(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('vCard', { key });
    return res.value;
  }

  /**
   * Returns the dimensionality of vectors in the set.
   */
  async vDim(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('vDim', { key });
    return res.value;
  }

  /**
   * Returns metadata about a vector set (metric, quantization, dimensions, etc.).
   */
  async vInfo(key: string): Promise<VInfoResult> {
    const res = await this.call<{
      exists: boolean;
      info: Array<{ field: string; value: Buffer }>;
    }>('vInfo', { key });
    const info: Record<string, string> = {};
    for (const fv of res.info) {
      info[fv.field] = fv.value.toString();
    }
    return { exists: res.exists, info };
  }

  // ---------------------------------------------------------------------------
  // pub/sub
  // ---------------------------------------------------------------------------

  /**
   * Publishes a message to a channel. Returns the number of subscribers that
   * received it.
   */
  async publish(channel: string, message: Buffer | string): Promise<number> {
    const res = await this.call<{ value: number }>('publish', {
      channel,
      message: Buffer.isBuffer(message) ? message : Buffer.from(message),
    });
    return res.value;
  }

  /**
   * Subscribes to one or more channels and/or patterns. Yields events until
   * the caller breaks out of the loop or calls `return()` on the iterator.
   *
   * Pass channel names in `channels` for exact matches and glob patterns in
   * `patterns` for wildcard matching.
   *
   * @example
   * ```ts
   * for await (const evt of client.subscribe(['news', 'alerts'])) {
   *   console.log(evt.channel, evt.data?.toString());
   *   if (evt.channel === 'alerts') break; // unsubscribes cleanly
   * }
   * ```
   */
  subscribe(channels: string[], patterns: string[] = []): AsyncIterable<SubscribeEvent> {
    const raw = this.stream<{
      kind: string;
      channel: string;
      data: Buffer;
      pattern?: string;
    }>('subscribe', { channels, patterns });

    return {
      [Symbol.asyncIterator]() {
        const iter = raw[Symbol.asyncIterator]();
        return {
          async next() {
            const { value, done } = await iter.next();
            if (done) return { value: undefined as unknown as SubscribeEvent, done: true };
            return {
              value: {
                kind: value.kind,
                channel: value.channel,
                data: value.data ?? null,
                pattern: value.pattern,
              } as SubscribeEvent,
              done: false,
            };
          },
          return() {
            return (
              iter.return?.() ??
              Promise.resolve({ value: undefined as unknown as SubscribeEvent, done: true })
            );
          },
        };
      },
    };
  }

  /**
   * Returns the names of all active channels, optionally filtered by `pattern`.
   */
  async pubSubChannels(pattern?: string): Promise<string[]> {
    const req: Record<string, unknown> = {};
    if (pattern != null) req.pattern = pattern;
    const res = await this.call<{ keys: string[] }>('pubSubChannels', req);
    return res.keys;
  }

  /**
   * Returns a map of channel name → subscriber count for the given channels.
   */
  async pubSubNumSub(...channels: string[]): Promise<Map<string, number>> {
    const res = await this.call<{ counts: Array<{ channel: string; count: number }> }>(
      'pubSubNumSub', { channels },
    );
    const result = new Map<string, number>();
    for (const c of res.counts) {
      result.set(c.channel, c.count);
    }
    return result;
  }

  /**
   * Returns the number of active pattern subscriptions across all clients.
   */
  async pubSubNumPat(): Promise<number> {
    const res = await this.call<{ value: number }>('pubSubNumPat', {});
    return res.value;
  }

  // ---------------------------------------------------------------------------
  // server
  // ---------------------------------------------------------------------------

  /**
   * Sends a PING. Returns `"PONG"` by default, or echoes `message` if provided.
   */
  async ping(message?: string): Promise<string> {
    const req: Record<string, unknown> = {};
    if (message != null) req.message = message;
    const res = await this.call<{ message: string }>('ping', req);
    return res.message;
  }

  /**
   * Sends `message` to the server and returns it back unchanged.
   */
  async echo(message: string): Promise<string> {
    const res = await this.call<{ message: string }>('echo', { message });
    return res.message;
  }

  /**
   * Removes all keys from the active database.
   * Pass `true` for a non-blocking background flush.
   */
  async flushDb(async_ = false): Promise<void> {
    await this.call('flushDb', { async: async_ });
  }

  /**
   * Returns the total number of keys across all shards.
   */
  async dbSize(): Promise<number> {
    const res = await this.call<{ value: number }>('dbSize', {});
    return res.value;
  }

  /**
   * Returns server statistics as a multi-line string.
   * Pass an optional `section` to limit the output (e.g. `"memory"`, `"server"`).
   */
  async info(section?: string): Promise<string> {
    const req: Record<string, unknown> = {};
    if (section != null) req.section = section;
    const res = await this.call<{ info: string }>('info', req);
    return res.info;
  }

  /**
   * Triggers an asynchronous snapshot of the dataset to disk. Returns the
   * server's status message.
   */
  async bgSave(): Promise<string> {
    const res = await this.call<{ status: string }>('bgSave', {});
    return res.status;
  }

  /**
   * Triggers an asynchronous rewrite of the append-only file. Returns the
   * server's status message.
   */
  async bgRewriteAof(): Promise<string> {
    const res = await this.call<{ status: string }>('bgRewriteAof', {});
    return res.status;
  }

  /**
   * Returns the current server time.
   */
  async time(): Promise<TimeResult> {
    const res = await this.call<{ seconds: number; microseconds: number }>('time', {});
    return { seconds: res.seconds, microseconds: res.microseconds };
  }

  /**
   * Returns the Unix timestamp of the last successful BGSAVE.
   */
  async lastSave(): Promise<number> {
    const res = await this.call<{ value: number }>('lastSave', {});
    return res.value;
  }

  // ---------------------------------------------------------------------------
  // slowlog
  // ---------------------------------------------------------------------------

  /**
   * Returns entries from the slow log. Pass `count` to limit the number returned.
   */
  async slowLogGet(count?: number): Promise<SlowLogEntry[]> {
    const req: Record<string, unknown> = {};
    if (count != null) req.count = count;
    const res = await this.call<{
      entries: Array<{
        id: number;
        timestampUnix: number;
        durationMicros: number;
        command: string;
      }>;
    }>('slowLogGet', req);
    return res.entries.map(e => ({
      id: e.id,
      timestamp: e.timestampUnix,
      durationMicros: e.durationMicros,
      command: e.command,
    }));
  }

  /**
   * Returns the number of entries currently in the slow log.
   */
  async slowLogLen(): Promise<number> {
    const res = await this.call<{ value: number }>('slowLogLen', {});
    return res.value;
  }

  /**
   * Clears all entries from the slow log.
   */
  async slowLogReset(): Promise<void> {
    await this.call('slowLogReset', {});
  }

  // ---------------------------------------------------------------------------
  // expiry (extended)
  // ---------------------------------------------------------------------------

  /**
   * Returns the absolute unix expiry timestamp in seconds.
   * Returns -1 if no expiry, -2 if the key does not exist.
   */
  async expiretime(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('expiretime', { key });
    return res.value;
  }

  /**
   * Returns the absolute unix expiry timestamp in milliseconds.
   * Returns -1 if no expiry, -2 if the key does not exist.
   */
  async pexpiretime(key: string): Promise<number> {
    const res = await this.call<{ value: number }>('pexpiretime', { key });
    return res.value;
  }

  /**
   * Sets the expiry to an absolute unix timestamp (seconds).
   * Returns `true` if the timeout was set.
   */
  async expireat(key: string, timestamp: number): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('expireat', { key, timestamp });
    return res.value;
  }

  /**
   * Sets the expiry to an absolute unix timestamp (milliseconds).
   * Returns `true` if the timeout was set.
   */
  async pexpireat(key: string, timestampMs: number): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('pexpireat', { key, timestampMs });
    return res.value;
  }

  // ---------------------------------------------------------------------------
  // strings (extended)
  // ---------------------------------------------------------------------------

  /**
   * Atomically sets `key` to `value` and returns the old value.
   * Returns `null` if the key did not exist.
   */
  async getset(key: string, value: Buffer | string): Promise<Buffer | null> {
    const res = await this.call<{ value?: Buffer }>('getset', {
      key,
      value: Buffer.isBuffer(value) ? value : Buffer.from(value),
    });
    return res.value ?? null;
  }

  /**
   * Sets multiple keys only if none of them exist.
   * Returns `true` if all keys were set, `false` if any existed.
   */
  async msetnx(pairs: Array<{ key: string; value: Buffer | string }>): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('msetnx', {
      pairs: pairs.map(p => ({
        key: p.key,
        value: Buffer.isBuffer(p.value) ? p.value : Buffer.from(p.value),
      })),
    });
    return res.value;
  }

  // ---------------------------------------------------------------------------
  // bitmaps
  // ---------------------------------------------------------------------------

  /**
   * Returns the bit at `offset` in the string stored at `key`.
   */
  async getbit(key: string, offset: number): Promise<number> {
    const res = await this.call<{ value: number }>('getbit', { key, offset });
    return res.value;
  }

  /**
   * Sets or clears the bit at `offset`. Returns the original bit value.
   */
  async setbit(key: string, offset: number, value: 0 | 1): Promise<number> {
    const res = await this.call<{ value: number }>('setbit', { key, offset, value });
    return res.value;
  }

  /**
   * Counts the number of set bits in the string at `key`.
   * Pass `range` as `{ start, end, unit }` to count within a range.
   */
  async bitcount(key: string, range?: { start: number; end: number; unit?: 'BYTE' | 'BIT' }): Promise<number> {
    const req: Record<string, unknown> = { key };
    if (range) {
      req.hasRange = true;
      req.start = range.start;
      req.end = range.end;
      req.unit = range.unit ?? 'BYTE';
    }
    const res = await this.call<{ value: number }>('bitcount', req);
    return res.value;
  }

  /**
   * Finds the first set (`bit=1`) or clear (`bit=0`) bit in the string at `key`.
   */
  async bitpos(key: string, bit: 0 | 1, range?: { start: number; end: number; unit?: 'BYTE' | 'BIT' }): Promise<number> {
    const req: Record<string, unknown> = { key, bit };
    if (range) {
      req.hasRange = true;
      req.start = range.start;
      req.end = range.end;
      req.unit = range.unit ?? 'BYTE';
    }
    const res = await this.call<{ value: number }>('bitpos', req);
    return res.value;
  }

  /**
   * Performs a bitwise operation between strings.
   * `op` is `"AND"`, `"OR"`, `"XOR"`, or `"NOT"`.
   * Result is stored at `dest`. Returns the length of the resulting string.
   */
  async bitop(op: 'AND' | 'OR' | 'XOR' | 'NOT', dest: string, keys: string[]): Promise<number> {
    const res = await this.call<{ value: number }>('bitop', { op, dest, keys });
    return res.value;
  }

  // ---------------------------------------------------------------------------
  // sets (extended)
  // ---------------------------------------------------------------------------

  /**
   * Atomically moves `member` from `source` to `destination`.
   * Returns `true` if the move succeeded.
   */
  async smove(source: string, destination: string, member: string): Promise<boolean> {
    const res = await this.call<{ value: boolean }>('smove', { source, destination, member });
    return res.value;
  }

  /**
   * Returns the cardinality of the intersection of `keys`.
   * `limit=0` means no limit.
   */
  async sintercard(keys: string[], limit = 0): Promise<number> {
    const res = await this.call<{ value: number }>('sintercard', { keys, limit });
    return res.value;
  }

  // ---------------------------------------------------------------------------
  // lists (extended)
  // ---------------------------------------------------------------------------

  /**
   * Pops up to `count` elements from the first non-empty list in `keys`.
   * `left=true` pops from the head, `left=false` from the tail.
   * Returns `null` if all lists are empty.
   */
  async lmpop(
    keys: string[],
    left: boolean,
    count = 1,
  ): Promise<{ key: string; elements: Buffer[] } | null> {
    const res = await this.call<{ found: boolean; key: string; elements: Buffer[] }>('lmpop', {
      keys,
      left,
      count,
    });
    if (!res.found) return null;
    return { key: res.key, elements: res.elements };
  }

  // ---------------------------------------------------------------------------
  // hash (extended)
  // ---------------------------------------------------------------------------

  /**
   * Returns random field(s) from the hash at `key`.
   * `count=undefined` returns a single field.
   * `withValues=true` returns interleaved field-value pairs.
   */
  async hrandfield(key: string, count?: number, withValues = false): Promise<Buffer[]> {
    const req: Record<string, unknown> = { key, withValues };
    if (count != null) {
      req.hasCount = true;
      req.count = count;
    }
    const res = await this.call<{ values: Buffer[] }>('hrandfield', req);
    return res.values ?? [];
  }

  // ---------------------------------------------------------------------------
  // sorted sets (extended)
  // ---------------------------------------------------------------------------

  /**
   * Pops up to `count` elements from the first non-empty sorted set in `keys`.
   * `min=true` pops minimum-score members. Returns `null` if all sorted sets are empty.
   */
  async zmpop(
    keys: string[],
    min: boolean,
    count = 1,
  ): Promise<{ key: string; members: Array<{ member: string; score: number }> } | null> {
    const res = await this.call<{
      found: boolean;
      key: string;
      members: Array<{ member: string; score: number }>;
    }>('zmpop', { keys, min, count });
    if (!res.found) return null;
    return { key: res.key, members: res.members };
  }

  /**
   * Returns random member(s) from the sorted set at `key`.
   * `count=undefined` returns a single member.
   * `withScores=true` returns interleaved member-score pairs.
   */
  async zrandmember(key: string, count?: number, withScores = false): Promise<Buffer[]> {
    const req: Record<string, unknown> = { key, withScores };
    if (count != null) {
      req.hasCount = true;
      req.count = count;
    }
    const res = await this.call<{ values: Buffer[] }>('zrandmember', req);
    return res.values ?? [];
  }
}
