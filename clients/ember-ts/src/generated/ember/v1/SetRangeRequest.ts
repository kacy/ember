// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface SetRangeRequest {
  'key'?: (string);
  'offset'?: (number | string | Long);
  'value'?: (Buffer | Uint8Array | string);
}

export interface SetRangeRequest__Output {
  'key'?: (string);
  'offset'?: (Long);
  'value'?: (Buffer);
}
