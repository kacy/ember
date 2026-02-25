// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface TimeResponse {
  'seconds'?: (number | string | Long);
  'microseconds'?: (number | string | Long);
}

export interface TimeResponse__Output {
  'seconds'?: (Long);
  'microseconds'?: (Long);
}
