// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface LIndexRequest {
  'key'?: (string);
  'index'?: (number | string | Long);
}

export interface LIndexRequest__Output {
  'key'?: (string);
  'index'?: (Long);
}
