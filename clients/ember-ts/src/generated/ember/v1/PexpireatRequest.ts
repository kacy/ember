// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface PexpireatRequest {
  'key'?: (string);
  'timestampMs'?: (number | string | Long);
}

export interface PexpireatRequest__Output {
  'key'?: (string);
  'timestampMs'?: (Long);
}
