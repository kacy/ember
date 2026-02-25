// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface DecrByRequest {
  'key'?: (string);
  'delta'?: (number | string | Long);
}

export interface DecrByRequest__Output {
  'key'?: (string);
  'delta'?: (Long);
}
