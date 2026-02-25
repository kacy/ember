// Original file: ../../proto/ember/v1/ember.proto

import type { VAddBatchEntry as _ember_v1_VAddBatchEntry, VAddBatchEntry__Output as _ember_v1_VAddBatchEntry__Output } from '../../ember/v1/VAddBatchEntry';
import type { VectorMetric as _ember_v1_VectorMetric, VectorMetric__Output as _ember_v1_VectorMetric__Output } from '../../ember/v1/VectorMetric';
import type { VectorQuantization as _ember_v1_VectorQuantization, VectorQuantization__Output as _ember_v1_VectorQuantization__Output } from '../../ember/v1/VectorQuantization';

export interface VAddBatchRequest {
  'key'?: (string);
  'entries'?: (_ember_v1_VAddBatchEntry)[];
  'metric'?: (_ember_v1_VectorMetric);
  'quantization'?: (_ember_v1_VectorQuantization);
  'connectivity'?: (number);
  'efConstruction'?: (number);
  '_connectivity'?: "connectivity";
  '_efConstruction'?: "efConstruction";
}

export interface VAddBatchRequest__Output {
  'key'?: (string);
  'entries'?: (_ember_v1_VAddBatchEntry__Output)[];
  'metric'?: (_ember_v1_VectorMetric__Output);
  'quantization'?: (_ember_v1_VectorQuantization__Output);
  'connectivity'?: (number);
  'efConstruction'?: (number);
}
