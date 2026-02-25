// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface LRangeRequest {
  'key'?: (string);
  'start'?: (number | string | Long);
  'stop'?: (number | string | Long);
}

export interface LRangeRequest__Output {
  'key'?: (string);
  'start'?: (Long);
  'stop'?: (Long);
}
