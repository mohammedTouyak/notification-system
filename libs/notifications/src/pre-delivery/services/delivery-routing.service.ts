import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { NotificationChannel } from '../../enums/notification-channel.enum';
import { KafkaService } from '../../kafka/kafka.service';
import { KafkaTopics } from '../../kafka/kafka-topics';
import { NotificationModel } from '../../models/notification.model';
import { NotificationDeliveryEvent } from '../models/notification-delivery-event.model';
import { BombardmentPolicyService } from './bombardment-policy.service';
import { DigestPolicyService } from './digest-policy.service';

@Injectable()
export class DeliveryRoutingService {
  private readonly logger = new Logger(DeliveryRoutingService.name);

  constructor(
    private readonly kafkaService: KafkaService,
    private readonly digestPolicyService: DigestPolicyService,
    private readonly bombardmentPolicyService: BombardmentPolicyService,
  ) {}

  async route(notification: NotificationModel): Promise<void> {
    const uniqueChannels = [...new Set(notification.channels)];

    for (const channel of uniqueChannels) {
      await this.routeChannel(notification, channel);
    }
  }

  private async routeChannel(
    notification: NotificationModel,
    channel: NotificationChannel,
  ): Promise<void> {
    const topic = this.resolveTopic(channel);

    if (!topic) {
      this.logger.warn(
        `[DELIVERY ROUTING] Unsupported channel=${String(
          channel,
        )} notificationId=${notification.id}`,
      );
      return;
    }

    const digestDecision = await this.digestPolicyService.check(
      notification,
      channel,
    );

    if (!digestDecision.accepted) {
      this.logger.warn(
        `[DELIVERY ROUTING] Skipped notificationId=${notification.id} channel=${channel} reason=${digestDecision.reason}`,
      );
      return;
    }

    const bombardmentDecision = await this.bombardmentPolicyService.check(
      notification,
      channel,
    );

    if (!bombardmentDecision.accepted) {
      this.logger.warn(
        `[DELIVERY ROUTING] Skipped notificationId=${notification.id} channel=${channel} reason=${bombardmentDecision.reason}`,
      );
      return;
    }

    await this.publishToDeliveryTopic(notification, channel, topic);
  }

  private resolveTopic(channel: NotificationChannel): string | null {
    switch (channel) {
      case NotificationChannel.IN_APP:
        return KafkaTopics.NOTIFICATIONS_IN_APP;

      case NotificationChannel.EMAIL:
        return KafkaTopics.NOTIFICATIONS_EMAIL;

      default:
        return null;
    }
  }

  private async publishToDeliveryTopic(
    notification: NotificationModel,
    channel: NotificationChannel,
    topic: string,
  ): Promise<void> {
    const deliveryEvent = this.buildDeliveryEvent(notification, channel);

    this.logger.log(
      `[DELIVERY ROUTING] Routing notificationId=${notification.id} channel=${channel} topic=${topic}`,
    );

    await this.kafkaService.emit(
      topic,
      deliveryEvent,
      notification.userId,
      {
        notificationId: notification.id,
        userId: notification.userId,
        appId: notification.appId,
        channel,
        source: 'notification-library',
      },
    );

    this.logger.log(
      `[DELIVERY ROUTING] Published notificationId=${notification.id} channel=${channel} topic=${topic}`,
    );
  }

  private buildDeliveryEvent(
    notification: NotificationModel,
    channel: NotificationChannel,
  ): NotificationDeliveryEvent {
    const templateKey = this.resolveTemplateKey(notification);

    return {
      eventId: randomUUID(),
      notificationId: notification.id,
      userId: notification.userId,
      appId: notification.appId,
      type: notification.type,
      channel,
      templateKey,
      data: notification.data,
      status: notification.status,
      createdAt: String(notification.createdAt ?? new Date().toISOString()),
      routedAt: new Date().toISOString(),
    };
  }

  private resolveTemplateKey(notification: NotificationModel): string {
    if (
      notification.data &&
      typeof notification.data === 'object' &&
      typeof notification.data.templateKey === 'string'
    ) {
      return notification.data.templateKey;
    }

    return notification.type;
  }
}