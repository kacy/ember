// Original file: ../../proto/ember/v1/ember.proto

import type { ScoreMember as _ember_v1_ScoreMember, ScoreMember__Output as _ember_v1_ScoreMember__Output } from '../../ember/v1/ScoreMember';
import type { Long } from '@grpc/proto-loader';

export interface ZScanResponse {
  'cursor'?: (number | string | Long);
  'members'?: (_ember_v1_ScoreMember)[];
}

export interface ZScanResponse__Output {
  'cursor'?: (Long);
  'members'?: (_ember_v1_ScoreMember__Output)[];
}
