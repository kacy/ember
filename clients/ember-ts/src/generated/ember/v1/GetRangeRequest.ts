// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface GetRangeRequest {
  'key'?: (string);
  'start'?: (number | string | Long);
  'end'?: (number | string | Long);
}

export interface GetRangeRequest__Output {
  'key'?: (string);
  'start'?: (Long);
  'end'?: (Long);
}
