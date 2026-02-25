// Original file: ../../proto/ember/v1/ember.proto


export interface VGetResponse {
  'exists'?: (boolean);
  'vector'?: (number | string)[];
  '_exists'?: "exists";
}

export interface VGetResponse__Output {
  'exists'?: (boolean);
  'vector'?: (number)[];
}
