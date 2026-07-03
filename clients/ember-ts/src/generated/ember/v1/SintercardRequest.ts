// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface SintercardRequest {
  'keys'?: (string)[];
  'limit'?: (number | string | Long);
}

export interface SintercardRequest__Output {
  'keys'?: (string)[];
  'limit'?: (Long);
}
