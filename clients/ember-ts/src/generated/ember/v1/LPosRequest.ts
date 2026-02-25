// Original file: ../../proto/ember/v1/ember.proto


export interface LPosRequest {
  'key'?: (string);
  'value'?: (Buffer | Uint8Array | string);
  'count'?: (number);
  '_count'?: "count";
}

export interface LPosRequest__Output {
  'key'?: (string);
  'value'?: (Buffer);
  'count'?: (number);
}
