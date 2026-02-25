// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface SScanRequest {
  'key'?: (string);
  'cursor'?: (number | string | Long);
  'pattern'?: (string);
  'count'?: (number);
  '_pattern'?: "pattern";
}

export interface SScanRequest__Output {
  'key'?: (string);
  'cursor'?: (Long);
  'pattern'?: (string);
  'count'?: (number);
}
