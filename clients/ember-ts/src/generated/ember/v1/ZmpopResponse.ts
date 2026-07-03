// Original file: ../../proto/ember/v1/ember.proto

import type { ScoreMember as _ember_v1_ScoreMember, ScoreMember__Output as _ember_v1_ScoreMember__Output } from '../../ember/v1/ScoreMember';

export interface ZmpopResponse {
  'found'?: (boolean);
  'key'?: (string);
  'members'?: (_ember_v1_ScoreMember)[];
}

export interface ZmpopResponse__Output {
  'found'?: (boolean);
  'key'?: (string);
  'members'?: (_ember_v1_ScoreMember__Output)[];
}
