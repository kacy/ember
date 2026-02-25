// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface ChannelCount {
  'channel'?: (string);
  'count'?: (number | string | Long);
}

export interface ChannelCount__Output {
  'channel'?: (string);
  'count'?: (Long);
}
