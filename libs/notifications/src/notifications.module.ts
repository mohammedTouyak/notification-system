import { Module } from '@nestjs/common';
import { KafkaService } from './kafka/kafka.service';
import { SendNotificationController } from './send-notification/send-notification.controller';
import { SendNotificationService } from './send-notification/send-notification.service';
import { SendNotificationUseCase } from './send-notification/send-notification.usecase';

@Module({
  imports: [],
  controllers: [SendNotificationController],
  providers: [SendNotificationUseCase, SendNotificationService, KafkaService],
  exports: [SendNotificationUseCase, SendNotificationService, KafkaService],
})
export class NotificationsModule {}