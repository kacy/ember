// Original file: ../../proto/ember/v1/ember.proto

export const VectorMetric = {
  VECTOR_METRIC_COSINE: 0,
  VECTOR_METRIC_EUCLIDEAN: 1,
  VECTOR_METRIC_INNER_PRODUCT: 2,
} as const;

export type VectorMetric =
  | 'VECTOR_METRIC_COSINE'
  | 0
  | 'VECTOR_METRIC_EUCLIDEAN'
  | 1
  | 'VECTOR_METRIC_INNER_PRODUCT'
  | 2

export type VectorMetric__Output = typeof VectorMetric[keyof typeof VectorMetric]
