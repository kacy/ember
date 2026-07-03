// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface BitposRequest {
  'key'?: (string);
  'bit'?: (number);
  'hasRange'?: (boolean);
  'start'?: (number | string | Long);
  'end'?: (number | string | Long);
  'unit'?: (string);
}

export interface BitposRequest__Output {
  'key'?: (string);
  'bit'?: (number);
  'hasRange'?: (boolean);
  'start'?: (Long);
  'end'?: (Long);
  'unit'?: (string);
}
