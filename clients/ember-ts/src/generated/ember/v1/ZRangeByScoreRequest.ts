// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ZRangeByScoreRequest {
  'key'?: (string);
  'min'?: (string);
  'max'?: (string);
  'offset'?: (number | string | Long);
  'count'?: (number | string | Long);
  'withScores'?: (boolean);
  '_offset'?: "offset";
  '_count'?: "count";
}

export interface ZRangeByScoreRequest__Output {
  'key'?: (string);
  'min'?: (string);
  'max'?: (string);
  'offset'?: (Long);
  'count'?: (Long);
  'withScores'?: (boolean);
}
