import { Module } from '@nestjs/common';
import { KafkaConsumerService } from './kafka/kafka-consumer.service';
import { KafkaService } from './kafka/kafka.service';
import { NotificationConsumer } from './pre-delivery/notification.consumer';
import { PreDeliveryOrchestratorService } from './pre-delivery/pre-delivery-orchestrator.service';
import { SendNotificationController } from './send-notification/send-notification.controller';
import { SendNotificationService } from './send-notification/send-notification.service';
import { SendNotificationUseCase } from './send-notification/send-notification.usecase';

@Module({
  imports: [],
  controllers: [SendNotificationController],
  providers: [
    SendNotificationUseCase,
    SendNotificationService,
    KafkaService,
    KafkaConsumerService,
    NotificationConsumer,
    PreDeliveryOrchestratorService,
  ],
  exports: [
    SendNotificationUseCase,
    SendNotificationService,
    KafkaService,
    KafkaConsumerService,
    PreDeliveryOrchestratorService,
  ],
})
export class NotificationsModule {}