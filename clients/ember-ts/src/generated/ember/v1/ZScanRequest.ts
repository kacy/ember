// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ZScanRequest {
  'key'?: (string);
  'cursor'?: (number | string | Long);
  'pattern'?: (string);
  'count'?: (number);
  '_pattern'?: "pattern";
}

export interface ZScanRequest__Output {
  'key'?: (string);
  'cursor'?: (Long);
  'pattern'?: (string);
  'count'?: (number);
}
