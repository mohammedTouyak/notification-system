import { Module } from '@nestjs/common';
import { KafkaConsumerService } from './kafka/kafka-consumer.service';
import { KafkaService } from './kafka/kafka.service';
import { NotificationConsumer } from './pre-delivery/notification.consumer';
import { PreDeliveryOrchestratorService } from './pre-delivery/pre-delivery-orchestrator.service';
import { BombardmentPolicyService } from './pre-delivery/services/bombardment-policy.service';
import { DeliveryRoutingService } from './pre-delivery/services/delivery-routing.service';
import { DigestPolicyService } from './pre-delivery/services/digest-policy.service';
import { MockUserValidationService } from './pre-delivery/services/mock-user-validation.service';
import { RedisService } from './redis/redis.service';
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
    RedisService,

    NotificationConsumer,
    PreDeliveryOrchestratorService,
    MockUserValidationService,
    DigestPolicyService,
    BombardmentPolicyService,
    DeliveryRoutingService,
  ],
  exports: [
    SendNotificationUseCase,
    SendNotificationService,

    KafkaService,
    KafkaConsumerService,
    RedisService,

    PreDeliveryOrchestratorService,
    MockUserValidationService,
    DigestPolicyService,
    BombardmentPolicyService,
    DeliveryRoutingService,
  ],
})
export class NotificationsModule {}