// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface BitcountRequest {
  'key'?: (string);
  'hasRange'?: (boolean);
  'start'?: (number | string | Long);
  'end'?: (number | string | Long);
  'unit'?: (string);
}

export interface BitcountRequest__Output {
  'key'?: (string);
  'hasRange'?: (boolean);
  'start'?: (Long);
  'end'?: (Long);
  'unit'?: (string);
}
