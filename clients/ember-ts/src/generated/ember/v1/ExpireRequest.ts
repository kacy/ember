// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ExpireRequest {
  'key'?: (string);
  'seconds'?: (number | string | Long);
}

export interface ExpireRequest__Output {
  'key'?: (string);
  'seconds'?: (Long);
}
