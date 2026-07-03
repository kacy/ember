// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ExpireatRequest {
  'key'?: (string);
  'timestamp'?: (number | string | Long);
}

export interface ExpireatRequest__Output {
  'key'?: (string);
  'timestamp'?: (Long);
}
