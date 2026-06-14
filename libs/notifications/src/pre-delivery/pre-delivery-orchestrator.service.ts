import { Injectable, Logger } from '@nestjs/common';
import { NotificationChannel } from '../enums/notification-channel.enum';
import { KafkaService } from '../kafka/kafka.service';
import { KafkaTopics } from '../kafka/kafka-topics';
import { NotificationModel } from '../models/notification.model';

@Injectable()
export class PreDeliveryOrchestratorService {
  private readonly logger = new Logger(PreDeliveryOrchestratorService.name);

  constructor(private readonly kafkaService: KafkaService) {}

  async handle(notification: NotificationModel): Promise<void> {
    this.logger.log(`[PRE-DELIVERY] Start notificationId=${notification.id}`);

    const isUserValid = this.validateUser(notification);

    if (!isUserValid) {
      this.logger.warn(
        `[PRE-DELIVERY] Rejected notificationId=${notification.id} reason=INVALID_USER`,
      );
      return;
    }

    const digestAllowed = this.checkDigest(notification);

    if (!digestAllowed) {
      this.logger.warn(
        `[PRE-DELIVERY] Rejected notificationId=${notification.id} reason=DIGEST_NOT_READY`,
      );
      return;
    }

    const bombardmentAllowed = this.checkBombardment(notification);

    if (!bombardmentAllowed) {
      this.logger.warn(
        `[PRE-DELIVERY] Rejected notificationId=${notification.id} reason=BOMBARDMENT_LIMIT_EXCEEDED`,
      );
      return;
    }

    await this.routeDeliveryTopics(notification);

    this.logger.log(`[PRE-DELIVERY] Completed notificationId=${notification.id}`);
  }

  private validateUser(notification: NotificationModel): boolean {
    if (!notification.userId) {
      this.logger.warn('[PRE-DELIVERY] User validation failed: missing userId');
      return false;
    }

    if (notification.userId === 'blocked') {
      this.logger.warn(
        `[PRE-DELIVERY] User validation failed userId=${notification.userId}`,
      );
      return false;
    }

    this.logger.log(
      `[PRE-DELIVERY] User validation passed userId=${notification.userId}`,
    );

    return true;
  }

  private checkDigest(notification: NotificationModel): boolean {
    this.logger.log(
      `[PRE-DELIVERY] Digest check passed notificationId=${notification.id}`,
    );

    return true;
  }

  private checkBombardment(notification: NotificationModel): boolean {
    const simulateBombardment =
      notification.data &&
      typeof notification.data === 'object' &&
      'simulateBombardment' in notification.data &&
      notification.data.simulateBombardment === true;

    if (simulateBombardment) {
      this.logger.warn(
        `[PRE-DELIVERY] Bombardment check failed notificationId=${notification.id}`,
      );
      return false;
    }

    this.logger.log(
      `[PRE-DELIVERY] Bombardment check passed notificationId=${notification.id}`,
    );

    return true;
  }

  private async routeDeliveryTopics(
    notification: NotificationModel,
  ): Promise<void> {
    if (notification.channels.includes(NotificationChannel.IN_APP)) {
      this.logger.log(
        `[PRE-DELIVERY] Routing channel=IN_APP topic=${KafkaTopics.NOTIFICATIONS_IN_APP}`,
      );

      await this.kafkaService.emit(
        KafkaTopics.NOTIFICATIONS_IN_APP,
        notification,
        notification.userId,
      );
    }

    if (notification.channels.includes(NotificationChannel.EMAIL)) {
      this.logger.log(
        `[PRE-DELIVERY] Routing channel=EMAIL topic=${KafkaTopics.NOTIFICATIONS_EMAIL}`,
      );

      await this.kafkaService.emit(
        KafkaTopics.NOTIFICATIONS_EMAIL,
        notification,
        notification.userId,
      );
    }
  }
}