// Original file: ../../proto/ember/v1/ember.proto

export const VectorQuantization = {
  VECTOR_QUANTIZATION_NONE: 0,
  VECTOR_QUANTIZATION_F16: 1,
  VECTOR_QUANTIZATION_I8: 2,
} as const;

export type VectorQuantization =
  | 'VECTOR_QUANTIZATION_NONE'
  | 0
  | 'VECTOR_QUANTIZATION_F16'
  | 1
  | 'VECTOR_QUANTIZATION_I8'
  | 2

export type VectorQuantization__Output = typeof VectorQuantization[keyof typeof VectorQuantization]
