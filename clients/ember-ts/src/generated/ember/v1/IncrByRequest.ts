// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface IncrByRequest {
  'key'?: (string);
  'delta'?: (number | string | Long);
}

export interface IncrByRequest__Output {
  'key'?: (string);
  'delta'?: (Long);
}
