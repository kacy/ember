// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface LSetRequest {
  'key'?: (string);
  'index'?: (number | string | Long);
  'value'?: (Buffer | Uint8Array | string);
}

export interface LSetRequest__Output {
  'key'?: (string);
  'index'?: (Long);
  'value'?: (Buffer);
}
