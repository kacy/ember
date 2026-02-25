// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ScanResponse {
  'cursor'?: (number | string | Long);
  'keys'?: (string)[];
}

export interface ScanResponse__Output {
  'cursor'?: (Long);
  'keys'?: (string)[];
}
