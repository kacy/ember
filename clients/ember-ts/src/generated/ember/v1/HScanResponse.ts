// Original file: ../../proto/ember/v1/ember.proto

import type { FieldValue as _ember_v1_FieldValue, FieldValue__Output as _ember_v1_FieldValue__Output } from '../../ember/v1/FieldValue';
import type { Long } from '@grpc/proto-loader';

export interface HScanResponse {
  'cursor'?: (number | string | Long);
  'fields'?: (_ember_v1_FieldValue)[];
}

export interface HScanResponse__Output {
  'cursor'?: (Long);
  'fields'?: (_ember_v1_FieldValue__Output)[];
}
