// Original file: ../../proto/ember/v1/ember.proto

export const ErrorKind = {
  ERROR_KIND_UNSPECIFIED: 0,
  ERROR_KIND_WRONG_TYPE: 1,
  ERROR_KIND_OUT_OF_MEMORY: 2,
  ERROR_KIND_INTERNAL: 3,
  ERROR_KIND_INVALID_ARGUMENT: 4,
} as const;

export type ErrorKind =
  | 'ERROR_KIND_UNSPECIFIED'
  | 0
  | 'ERROR_KIND_WRONG_TYPE'
  | 1
  | 'ERROR_KIND_OUT_OF_MEMORY'
  | 2
  | 'ERROR_KIND_INTERNAL'
  | 3
  | 'ERROR_KIND_INVALID_ARGUMENT'
  | 4

export type ErrorKind__Output = typeof ErrorKind[keyof typeof ErrorKind]
