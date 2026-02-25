// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface LRemRequest {
  'key'?: (string);
  'count'?: (number | string | Long);
  'value'?: (Buffer | Uint8Array | string);
}

export interface LRemRequest__Output {
  'key'?: (string);
  'count'?: (Long);
  'value'?: (Buffer);
}
