// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ZRevRangeRequest {
  'key'?: (string);
  'start'?: (number | string | Long);
  'stop'?: (number | string | Long);
  'withScores'?: (boolean);
}

export interface ZRevRangeRequest__Output {
  'key'?: (string);
  'start'?: (Long);
  'stop'?: (Long);
  'withScores'?: (boolean);
}
