// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface GetExRequest {
  'key'?: (string);
  'expireSeconds'?: (number | string | Long);
  'expireMillis'?: (number | string | Long);
  'persist'?: (boolean);
}

export interface GetExRequest__Output {
  'key'?: (string);
  'expireSeconds'?: (Long);
  'expireMillis'?: (Long);
  'persist'?: (boolean);
}
