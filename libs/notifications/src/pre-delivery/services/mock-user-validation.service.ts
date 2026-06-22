import { Injectable, Logger } from '@nestjs/common';
import { NotificationModel } from '../../models/notification.model';
import {
  PreDeliveryDecision,
  PreDeliveryRejectionReason,
} from '../types/pre-delivery-decision.type';

@Injectable()
export class MockUserValidationService {
  private readonly logger = new Logger(MockUserValidationService.name);

  private readonly blockedUserIds = new Set(
    (process.env.MOCK_BLOCKED_USER_IDS ?? 'blocked')
      .split(',')
      .map((userId) => userId.trim())
      .filter(Boolean),
  );

  validate(notification: NotificationModel): PreDeliveryDecision {
    if (!notification.userId) {
      this.logger.warn('[MOCK USER VALIDATION] Missing userId');

      return {
        accepted: false,
        reason: PreDeliveryRejectionReason.MISSING_USER_ID,
        message: 'Notification userId is missing',
      };
    }

    if (this.blockedUserIds.has(notification.userId)) {
      this.logger.warn(
        `[MOCK USER VALIDATION] User blocked userId=${notification.userId}`,
      );

      return {
        accepted: false,
        reason: PreDeliveryRejectionReason.INVALID_USER,
        message: 'User is blocked by mock IAM validation',
      };
    }

    this.logger.log(
      `[MOCK USER VALIDATION] Passed userId=${notification.userId}`,
    );

    return {
      accepted: true,
    };
  }
}