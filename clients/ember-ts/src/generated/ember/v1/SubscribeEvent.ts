// Original file: ../../proto/ember/v1/ember.proto


export interface SubscribeEvent {
  'kind'?: (string);
  'channel'?: (string);
  'data'?: (Buffer | Uint8Array | string);
  'pattern'?: (string);
  '_pattern'?: "pattern";
}

export interface SubscribeEvent__Output {
  'kind'?: (string);
  'channel'?: (string);
  'data'?: (Buffer);
  'pattern'?: (string);
}
