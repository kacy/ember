// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface SetbitRequest {
  'key'?: (string);
  'offset'?: (number | string | Long);
  'value'?: (number);
}

export interface SetbitRequest__Output {
  'key'?: (string);
  'offset'?: (Long);
  'value'?: (number);
}
