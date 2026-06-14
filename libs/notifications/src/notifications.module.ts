import { Module } from '@nestjs/common';
import { SendNotificationController } from './send-notification/send-notification.controller';
import { SendNotificationService } from './send-notification/send-notification.service';
import { SendNotificationUseCase } from './send-notification/send-notification.usecase';

@Module({
  imports: [],
  controllers: [SendNotificationController],
  providers: [SendNotificationUseCase, SendNotificationService],
  exports: [SendNotificationUseCase, SendNotificationService],
})
export class NotificationsModule {}