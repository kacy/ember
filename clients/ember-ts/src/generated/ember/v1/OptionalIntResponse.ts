// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface OptionalIntResponse {
  'value'?: (number | string | Long);
  '_value'?: "value";
}

export interface OptionalIntResponse__Output {
  'value'?: (Long);
}
