import { Injectable, Logger } from '@nestjs/common';
import { NotificationChannel } from '../../enums/notification-channel.enum';
import { NotificationModel } from '../../models/notification.model';
import { RedisService } from '../../redis/redis.service';
import {
  PreDeliveryDecision,
  PreDeliveryRejectionReason,
} from '../types/pre-delivery-decision.type';

interface BombardmentLimit {
  max: number;
  windowSeconds: number;
}

@Injectable()
export class BombardmentPolicyService {
  private readonly logger = new Logger(BombardmentPolicyService.name);

  constructor(private readonly redisService: RedisService) {}

  async check(
    notification: NotificationModel,
    channel: NotificationChannel,
  ): Promise<PreDeliveryDecision> {
    if (!this.isBombardmentEnabled()) {
      return { accepted: true };
    }

    const limit = this.getLimit(channel);
    const key = this.buildBombardmentKey(notification, channel);

    const currentCount = await this.redisService.incrementWithExpire(
      key,
      limit.windowSeconds,
    );

    this.logger.log(
      `[BOMBARDMENT CHECK] userId=${notification.userId} channel=${channel} count=${currentCount}/${limit.max} window=${limit.windowSeconds}s`,
    );

    if (currentCount > limit.max) {
      this.logger.warn(
        `[BOMBARDMENT CHECK] Failed notificationId=${notification.id} userId=${notification.userId} channel=${channel}`,
      );

      return {
        accepted: false,
        reason: PreDeliveryRejectionReason.BOMBARDMENT_LIMIT_EXCEEDED,
        message: `Bombardment limit exceeded for channel ${channel}`,
        metadata: {
          key,
          currentCount,
          max: limit.max,
          windowSeconds: limit.windowSeconds,
        },
      };
    }

    return {
      accepted: true,
      metadata: {
        key,
        currentCount,
        max: limit.max,
        windowSeconds: limit.windowSeconds,
      },
    };
  }

  private buildBombardmentKey(
    notification: NotificationModel,
    channel: NotificationChannel,
  ): string {
    return `bombardment:${notification.appId}:${notification.userId}:${channel}`;
  }

  private getLimit(channel: NotificationChannel): BombardmentLimit {
    if (channel === NotificationChannel.EMAIL) {
      return {
        max: this.getNumberEnv('NOTIFICATION_BOMBARDMENT_EMAIL_MAX', 3),
        windowSeconds: this.getNumberEnv(
          'NOTIFICATION_BOMBARDMENT_EMAIL_WINDOW_SECONDS',
          3600,
        ),
      };
    }

    return {
      max: this.getNumberEnv('NOTIFICATION_BOMBARDMENT_IN_APP_MAX', 5),
      windowSeconds: this.getNumberEnv(
        'NOTIFICATION_BOMBARDMENT_IN_APP_WINDOW_SECONDS',
        60,
      ),
    };
  }

  private isBombardmentEnabled(): boolean {
    return process.env.NOTIFICATION_BOMBARDMENT_ENABLED !== 'false';
  }

  private getNumberEnv(name: string, defaultValue: number): number {
    const value = process.env[name];

    if (!value) {
      return defaultValue;
    }

    const parsedValue = Number(value);

    return Number.isNaN(parsedValue) ? defaultValue : parsedValue;
  }
}