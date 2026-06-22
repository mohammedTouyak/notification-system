import { Injectable, Logger } from '@nestjs/common';
import { NotificationChannel } from '../../enums/notification-channel.enum';
import { NotificationModel } from '../../models/notification.model';
import { RedisService } from '../../redis/redis.service';
import {
  PreDeliveryDecision,
  PreDeliveryRejectionReason,
} from '../types/pre-delivery-decision.type';

interface DigestStoredNotification {
  notificationId: string;
  userId: string;
  appId: string;
  type: string;
  channel: NotificationChannel;
  data: Record<string, unknown>;
  queuedAt: string;
}

@Injectable()
export class DigestPolicyService {
  private readonly logger = new Logger(DigestPolicyService.name);

  constructor(private readonly redisService: RedisService) {}

  async check(
    notification: NotificationModel,
    channel: NotificationChannel,
  ): Promise<PreDeliveryDecision> {
    if (!this.isDigestEnabled()) {
      return { accepted: true };
    }

    if (!this.shouldQueueForDigest(notification, channel)) {
      this.logger.log(
        `[DIGEST CHECK] Immediate delivery notificationId=${notification.id} channel=${channel}`,
      );

      return { accepted: true };
    }

    const digestKey = this.buildDigestKey(notification, channel);

    const storedNotification: DigestStoredNotification = {
      notificationId: notification.id,
      userId: notification.userId,
      appId: notification.appId,
      type: notification.type,
      channel,
      data: notification.data,
      queuedAt: new Date().toISOString(),
    };

    await this.redisService.addToSortedSetWithTtl(
      digestKey,
      Date.now(),
      storedNotification,
      this.getDigestTtlSeconds(),
    );

    this.logger.log(
      `[DIGEST CHECK] Queued notificationId=${notification.id} channel=${channel} key=${digestKey}`,
    );

    return {
      accepted: false,
      reason: PreDeliveryRejectionReason.DIGEST_QUEUED,
      message: 'Notification queued for digest delivery',
      metadata: {
        digestKey,
        channel,
      },
    };
  }

  private shouldQueueForDigest(
    notification: NotificationModel,
    channel: NotificationChannel,
  ): boolean {
    if (!notification.data || typeof notification.data !== 'object') {
      return false;
    }

    const digest = notification.data.digest;
    const deliveryMode = notification.data.deliveryMode;
    const digestChannels = notification.data.digestChannels;

    if (deliveryMode === 'IMMEDIATE') {
      return false;
    }

    if (Array.isArray(digestChannels)) {
      return digestChannels.includes(channel);
    }

    if (digest === true && channel === NotificationChannel.EMAIL) {
      return true;
    }

    if (deliveryMode === 'DIGEST' && channel === NotificationChannel.EMAIL) {
      return true;
    }

    return false;
  }

  private buildDigestKey(
    notification: NotificationModel,
    channel: NotificationChannel,
  ): string {
    return `digest:${notification.appId}:${notification.userId}:${channel}`;
  }

  private isDigestEnabled(): boolean {
    return process.env.NOTIFICATION_DIGEST_ENABLED !== 'false';
  }

  private getDigestTtlSeconds(): number {
    const value = process.env.NOTIFICATION_DIGEST_TTL_SECONDS;

    if (!value) {
      return 86_400;
    }

    const parsedValue = Number(value);

    return Number.isNaN(parsedValue) ? 86_400 : parsedValue;
  }
}