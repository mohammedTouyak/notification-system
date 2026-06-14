import { Body, Controller, Post } from '@nestjs/common';
import { CreateNotificationDto } from '../dtos/create-notification.dto';
import {
  SendNotificationResult,
  SendNotificationUseCase,
} from './send-notification.usecase';

@Controller('notifications')
export class SendNotificationController {
  constructor(
    private readonly sendNotificationUseCase: SendNotificationUseCase,
  ) {}

  @Post()
  async sendNotification(
    @Body() createNotificationDto: CreateNotificationDto,
  ): Promise<SendNotificationResult> {
    return this.sendNotificationUseCase.execute(createNotificationDto);
  }
}