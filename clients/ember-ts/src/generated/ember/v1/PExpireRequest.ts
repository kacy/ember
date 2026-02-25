// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface PExpireRequest {
  'key'?: (string);
  'milliseconds'?: (number | string | Long);
}

export interface PExpireRequest__Output {
  'key'?: (string);
  'milliseconds'?: (Long);
}
