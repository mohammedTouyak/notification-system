import { Injectable, Logger } from '@nestjs/common';
import { NotificationModel } from '../models/notification.model';

@Injectable()
export class PreDeliveryOrchestratorService {
  private readonly logger = new Logger(PreDeliveryOrchestratorService.name);

  async handle(notification: NotificationModel): Promise<void> {
    this.logger.log(
      `[PRE-DELIVERY] Received notificationId=${notification.id}`,
    );

    this.logger.log(
      `[PRE-DELIVERY] userId=${notification.userId} appId=${notification.appId} type=${notification.type}`,
    );

    this.logger.log(
      `[PRE-DELIVERY] channels=${notification.channels.join(',')}`,
    );

    this.logger.log(
      '[PRE-DELIVERY] Business rules will be implemented in TASK 03',
    );
  }
}