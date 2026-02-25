// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface SetRequest {
  'key'?: (string);
  'value'?: (Buffer | Uint8Array | string);
  'expireSeconds'?: (number | string | Long);
  'expireMillis'?: (number | string | Long);
  'nx'?: (boolean);
  'xx'?: (boolean);
}

export interface SetRequest__Output {
  'key'?: (string);
  'value'?: (Buffer);
  'expireSeconds'?: (Long);
  'expireMillis'?: (Long);
  'nx'?: (boolean);
  'xx'?: (boolean);
}
