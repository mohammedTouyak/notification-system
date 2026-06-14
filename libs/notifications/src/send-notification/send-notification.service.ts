import { Injectable, Logger } from '@nestjs/common';
import { randomUUID } from 'crypto';
import { CreateNotificationDto } from '../dtos/create-notification.dto';
import { KafkaService } from '../kafka/kafka.service';
import { KafkaTopics } from '../kafka/kafka-topics';
import { NotificationModel } from '../models/notification.model';

@Injectable()
export class SendNotificationService {
  private readonly logger = new Logger(SendNotificationService.name);

  constructor(private readonly kafkaService: KafkaService) {}

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
    this.logger.log(`[NOTIFICATION CREATED] id=${notification.id}`);
    this.logger.log(`userId=${notification.userId}`);
    this.logger.log(`appId=${notification.appId}`);
    this.logger.log(`type=${notification.type}`);
    this.logger.log(`channels=${notification.channels.join(',')}`);
    this.logger.log(`status=${notification.status}`);

    await this.kafkaService.emit(
      KafkaTopics.NOTIFICATIONS,
      notification,
      notification.userId,
    );

    // NB: pourquoi utiliser key = userId ? 
    // Réponse: La key sert à aider Kafka à choisir la partition. Kafka peut mettre les messages du même userId dans la même partition.
    // Cela permet de garantir que les messages pour le même utilisateur sont traités dans l'ordre.

    return notification;
  }
}