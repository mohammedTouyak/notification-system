import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { CreateNotificationDto } from '../dtos/create-notification.dto';
import { NotificationModel } from '../models/notification.model';

@Injectable()
export class SendNotificationService {
  private readonly logger = new Logger(SendNotificationService.name);

  async processNotification(
    createNotificationDto: CreateNotificationDto,
  ): Promise<NotificationModel> {
    const notification = new NotificationModel({
      id: randomUUID(),
      userId: createNotificationDto.userId,
      appId: createNotificationDto.appId,
      type: createNotificationDto.type,
      channels: createNotificationDto.channels,
      data: createNotificationDto.data,
    });

    this.logger.log('[NOTIFICATION REQUEST RECEIVED]');
    this.logger.log(`userId=${notification.userId}`);
    this.logger.log(`appId=${notification.appId}`);
    this.logger.log(`type=${notification.type}`);
    this.logger.log(`channels=${notification.channels.join(',')}`);
    this.logger.log(`status=${notification.status}`);
    this.logger.log(`data=${JSON.stringify(notification.data)}`);

    return notification;
  }
}