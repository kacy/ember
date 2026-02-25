// Original file: ../../proto/ember/v1/ember.proto

import type { Long } from '@grpc/proto-loader';

export interface SlowLogEntry {
  'id'?: (number | string | Long);
  'timestampUnix'?: (number | string | Long);
  'durationMicros'?: (number | string | Long);
  'command'?: (string);
}

export interface SlowLogEntry__Output {
  'id'?: (Long);
  'timestampUnix'?: (Long);
  'durationMicros'?: (Long);
  'command'?: (string);
}
