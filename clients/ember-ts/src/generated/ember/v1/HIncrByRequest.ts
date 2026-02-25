// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface HIncrByRequest {
  'key'?: (string);
  'field'?: (string);
  'delta'?: (number | string | Long);
}

export interface HIncrByRequest__Output {
  'key'?: (string);
  'field'?: (string);
  'delta'?: (Long);
}
