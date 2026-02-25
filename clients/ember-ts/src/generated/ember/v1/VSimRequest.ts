// Original file: ../../proto/ember/v1/ember.proto


export interface VSimRequest {
  'key'?: (string);
  'query'?: (number | string)[];
  'count'?: (number);
  'efSearch'?: (number);
  '_efSearch'?: "efSearch";
}

export interface VSimRequest__Output {
  'key'?: (string);
  'query'?: (number)[];
  'count'?: (number);
  'efSearch'?: (number);
}
