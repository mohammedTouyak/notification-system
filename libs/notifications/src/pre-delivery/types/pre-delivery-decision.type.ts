export enum PreDeliveryRejectionReason {
  MISSING_USER_ID = 'MISSING_USER_ID',
  INVALID_USER = 'INVALID_USER',
  NO_CHANNELS = 'NO_CHANNELS',
  DIGEST_QUEUED = 'DIGEST_QUEUED',
  DIGEST_NOT_READY = 'DIGEST_NOT_READY',
  BOMBARDMENT_LIMIT_EXCEEDED = 'BOMBARDMENT_LIMIT_EXCEEDED',
}

export interface PreDeliveryDecision {
  accepted: boolean;
  reason?: PreDeliveryRejectionReason;
  message?: string;
  metadata?: Record<string, unknown>;
}