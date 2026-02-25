// Original file: ../../proto/ember/v1/ember.proto


export interface PublishRequest {
  'channel'?: (string);
  'message'?: (Buffer | Uint8Array | string);
}

export interface PublishRequest__Output {
  'channel'?: (string);
  'message'?: (Buffer);
}
