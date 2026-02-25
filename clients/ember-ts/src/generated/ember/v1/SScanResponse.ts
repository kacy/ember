// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface SScanResponse {
  'cursor'?: (number | string | Long);
  'members'?: (string)[];
}

export interface SScanResponse__Output {
  'cursor'?: (Long);
  'members'?: (string)[];
}
