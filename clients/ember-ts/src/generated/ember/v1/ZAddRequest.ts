// Original file: ../../proto/ember/v1/ember.proto

import type { ScoreMember as _ember_v1_ScoreMember, ScoreMember__Output as _ember_v1_ScoreMember__Output } from '../../ember/v1/ScoreMember';

export interface ZAddRequest {
  'key'?: (string);
  'members'?: (_ember_v1_ScoreMember)[];
  'nx'?: (boolean);
  'xx'?: (boolean);
  'gt'?: (boolean);
  'lt'?: (boolean);
  'ch'?: (boolean);
}

export interface ZAddRequest__Output {
  'key'?: (string);
  'members'?: (_ember_v1_ScoreMember__Output)[];
  'nx'?: (boolean);
  'xx'?: (boolean);
  'gt'?: (boolean);
  'lt'?: (boolean);
  'ch'?: (boolean);
}
