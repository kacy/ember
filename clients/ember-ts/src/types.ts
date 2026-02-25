/**
 * Options passed to the EmberClient constructor.
 */
export interface ClientOptions {
  /** authentication password, sent as the `authorization` metadata header. */
  password?: string;
}

/**
 * Options for the SET command.
 */
export interface SetOptions {
  /** expire the key after this many seconds. */
  ex?: number;
  /** expire the key after this many milliseconds. */
  px?: number;
  /** only set the key if it does not already exist. */
  nx?: boolean;
  /** only set the key if it already exists. */
  xx?: boolean;
}

/**
 * Options for the GETEX command.
 */
export interface GetExOptions {
  /** set expiry in seconds. */
  ex?: number;
  /** set expiry in milliseconds (takes precedence over `ex`). */
  px?: number;
  /** remove the existing expiry, making the key persistent. */
  persist?: boolean;
}

/**
 * Options for SCAN, HSCAN, SSCAN, and ZSCAN commands.
 */
export interface ScanOptions {
  /** glob-style pattern to filter returned keys/members. */
  pattern?: string;
  /** hint for how many items to return per call. the server may return more or fewer. */
  count?: number;
}

/**
 * Options for the ZADD command.
 */
export interface ZAddOptions {
  /** only add new members; never update scores of existing members. */
  nx?: boolean;
  /** only update existing members; never add new members. */
  xx?: boolean;
  /** only update if the new score is greater than the current score. */
  gt?: boolean;
  /** only update if the new score is less than the current score. */
  lt?: boolean;
  /** return the number of elements changed (added + updated) instead of only added. */
  ch?: boolean;
}

/**
 * Options for ZRANGEBYSCORE and ZREVRANGEBYSCORE commands.
 */
export interface ZRangeByScoreOptions {
  /** starting offset for pagination. */
  offset?: number;
  /** maximum number of results to return. */
  count?: number;
  /** include scores in the response. */
  withScores?: boolean;
}

/**
 * Distance metric used for vector similarity search.
 */
export type VectorMetric =
  | 'VECTOR_METRIC_COSINE'
  | 'VECTOR_METRIC_EUCLIDEAN'
  | 'VECTOR_METRIC_INNER_PRODUCT';

/**
 * Quantization strategy for compressing stored vectors.
 */
export type VectorQuantization =
  | 'VECTOR_QUANTIZATION_NONE'
  | 'VECTOR_QUANTIZATION_F16'
  | 'VECTOR_QUANTIZATION_I8';

/**
 * Options for the VADD command.
 */
export interface VAddOptions {
  /** distance metric for the vector set. defaults to cosine. */
  metric?: VectorMetric;
  /** quantization strategy to compress stored vectors. */
  quantization?: VectorQuantization;
  /** HNSW M parameter — number of connections per node. */
  connectivity?: number;
  /** HNSW ef_construction parameter — search width during index build. */
  efConstruction?: number;
}

/**
 * Options for the VADDBATCH command.
 */
export interface VAddBatchOptions {
  /** distance metric for the vector set. defaults to cosine. */
  metric?: VectorMetric;
  /** quantization strategy to compress stored vectors. */
  quantization?: VectorQuantization;
  /** HNSW M parameter. */
  connectivity?: number;
  /** HNSW ef_construction parameter. */
  efConstruction?: number;
}

/**
 * A single entry for VADDBATCH.
 */
export interface VAddBatchEntry {
  /** the element name. */
  element: string;
  /** the vector as an array of floats. */
  vector: number[];
}

/**
 * Options for the VSIM command.
 */
export interface VSimOptions {
  /** ef_search parameter — controls recall vs. latency trade-off. */
  efSearch?: number;
}

// ---------------------------------------------------------------------------
// result types
// ---------------------------------------------------------------------------

/**
 * A sorted-set member paired with its score.
 */
export interface ScoreMember {
  member: string;
  score: number;
}

/**
 * A single result from VSIM (vector similarity search).
 */
export interface VSimResult {
  element: string;
  distance: number;
}

/**
 * An entry from the slow log.
 */
export interface SlowLogEntry {
  id: number;
  /** unix timestamp when the command was executed. */
  timestamp: number;
  /** how long the command took, in microseconds. */
  durationMicros: number;
  /** the command string as logged by the server. */
  command: string;
}

/**
 * An event received on a pub/sub subscription.
 */
export interface SubscribeEvent {
  /** "message" for exact channel matches, "pmessage" for pattern matches. */
  kind: string;
  /** the channel the message was published to. */
  channel: string;
  /** the message payload. */
  data: Buffer | null;
  /** the pattern that matched (only present for pmessage events). */
  pattern?: string;
}

/**
 * A page of keys returned by SCAN.
 */
export interface ScanPage {
  /** the cursor for the next call. 0 means the scan is complete. */
  cursor: number;
  keys: string[];
}

/**
 * A page of field-value pairs returned by HSCAN.
 */
export interface HScanPage {
  /** the cursor for the next call. 0 means the scan is complete. */
  cursor: number;
  fields: Record<string, Buffer>;
}

/**
 * A page of score-member pairs returned by ZSCAN.
 */
export interface ZScanPage {
  /** the cursor for the next call. 0 means the scan is complete. */
  cursor: number;
  members: ScoreMember[];
}

/**
 * A page of set members returned by SSCAN.
 */
export interface SScanPage {
  /** the cursor for the next call. 0 means the scan is complete. */
  cursor: number;
  members: string[];
}

/**
 * The current server time, as returned by TIME.
 */
export interface TimeResult {
  /** unix timestamp in whole seconds. */
  seconds: number;
  /** microseconds offset within the current second. */
  microseconds: number;
}

/**
 * The result of VGET — the stored vector for a named element.
 */
export interface VGetResult {
  /** false if the element does not exist in the vector set. */
  exists: boolean;
  /** the stored vector as an array of floats. empty if the element does not exist. */
  vector: number[];
}

/**
 * Metadata about a vector set, returned by VINFO.
 */
export interface VInfoResult {
  /** false if the vector set key does not exist. */
  exists: boolean;
  /** metadata fields (metric, dimensions, capacity, etc.) as string values. */
  info: Record<string, string>;
}
