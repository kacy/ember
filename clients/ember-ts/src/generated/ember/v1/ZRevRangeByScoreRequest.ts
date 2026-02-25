// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ZRevRangeByScoreRequest {
  'key'?: (string);
  'max'?: (string);
  'min'?: (string);
  'offset'?: (number | string | Long);
  'count'?: (number | string | Long);
  'withScores'?: (boolean);
  '_offset'?: "offset";
  '_count'?: "count";
}

export interface ZRevRangeByScoreRequest__Output {
  'key'?: (string);
  'max'?: (string);
  'min'?: (string);
  'offset'?: (Long);
  'count'?: (Long);
  'withScores'?: (boolean);
}
