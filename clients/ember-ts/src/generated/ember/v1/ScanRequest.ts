// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ScanRequest {
  'cursor'?: (number | string | Long);
  'count'?: (number);
  'pattern'?: (string);
  '_pattern'?: "pattern";
}

export interface ScanRequest__Output {
  'cursor'?: (Long);
  'count'?: (number);
  'pattern'?: (string);
}
