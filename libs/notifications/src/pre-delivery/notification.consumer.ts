import { Injectable, Logger, OnModuleInit } from '@nestjs/common';
import { KafkaConsumerService } from '../kafka/kafka-consumer.service';
import { KafkaTopics } from '../kafka/kafka-topics';
import { NotificationModel } from '../models/notification.model';
import { PreDeliveryOrchestratorService } from './pre-delivery-orchestrator.service';

@Injectable()
export class NotificationConsumer implements OnModuleInit {
  private readonly logger = new Logger(NotificationConsumer.name);

  constructor(
    private readonly kafkaConsumerService: KafkaConsumerService,
    private readonly preDeliveryOrchestratorService: PreDeliveryOrchestratorService,
  ) {}

  async onModuleInit(): Promise<void> {
    await this.kafkaConsumerService.consume<NotificationModel>(
      KafkaTopics.NOTIFICATIONS,
      async (notification) => {
        this.logger.log(
          `[NOTIFICATION CONSUMER] Consumed notificationId=${notification.id}`,
        );

        this.logger.log(
          '[NOTIFICATION CONSUMER] Delegating to pre-delivery orchestrator',
        );

        await this.preDeliveryOrchestratorService.handle(notification);
      },
    );
  }
}