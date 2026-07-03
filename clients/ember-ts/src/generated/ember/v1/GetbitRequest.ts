// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface GetbitRequest {
  'key'?: (string);
  'offset'?: (number | string | Long);
}

export interface GetbitRequest__Output {
  'key'?: (string);
  'offset'?: (Long);
}
