import { Injectable } from '@nestjs/common';
import { CreateNotificationDto } from '../dtos/create-notification.dto';
import { NotificationStatus } from '../enums/notification-status.enum';
import { SendNotificationService } from './send-notification.service';

export interface SendNotificationResult {
  message: string;
  status: NotificationStatus;
  notificationId: string;
}

@Injectable()
export class SendNotificationUseCase {
  constructor(
    private readonly sendNotificationService: SendNotificationService,
  ) {}

  async execute(
    createNotificationDto: CreateNotificationDto,
  ): Promise<SendNotificationResult> {
    const notification =
      await this.sendNotificationService.processNotification(
        createNotificationDto,
      );

    return {
      message: 'Notification request received',
      status: notification.status,
      notificationId: notification.id,
    };
  }
}