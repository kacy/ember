// Original file: ../../proto/ember/v1/ember.proto


export interface LPushRequest {
  'key'?: (string);
  'values'?: (Buffer | Uint8Array | string)[];
}

export interface LPushRequest__Output {
  'key'?: (string);
  'values'?: (Buffer)[];
}
