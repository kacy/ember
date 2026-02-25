// Original file: ../../proto/ember/v1/ember.proto

import type { VectorMetric as _ember_v1_VectorMetric, VectorMetric__Output as _ember_v1_VectorMetric__Output } from '../../ember/v1/VectorMetric';
import type { VectorQuantization as _ember_v1_VectorQuantization, VectorQuantization__Output as _ember_v1_VectorQuantization__Output } from '../../ember/v1/VectorQuantization';

export interface VAddRequest {
  'key'?: (string);
  'element'?: (string);
  'vector'?: (number | string)[];
  'metric'?: (_ember_v1_VectorMetric);
  'quantization'?: (_ember_v1_VectorQuantization);
  'connectivity'?: (number);
  'efConstruction'?: (number);
  '_connectivity'?: "connectivity";
  '_efConstruction'?: "efConstruction";
}

export interface VAddRequest__Output {
  'key'?: (string);
  'element'?: (string);
  'vector'?: (number)[];
  'metric'?: (_ember_v1_VectorMetric__Output);
  'quantization'?: (_ember_v1_VectorQuantization__Output);
  'connectivity'?: (number);
  'efConstruction'?: (number);
}
