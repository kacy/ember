// Original file: ../../proto/ember/v1/ember.proto


export interface LInsertRequest {
  'key'?: (string);
  'before'?: (boolean);
  'pivot'?: (Buffer | Uint8Array | string);
  'value'?: (Buffer | Uint8Array | string);
}

export interface LInsertRequest__Output {
  'key'?: (string);
  'before'?: (boolean);
  'pivot'?: (Buffer);
  'value'?: (Buffer);
}
