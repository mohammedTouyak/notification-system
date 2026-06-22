import { Injectable, Logger } from '@nestjs/common';
import { NotificationModel } from '../models/notification.model';
import { DeliveryRoutingService } from './services/delivery-routing.service';
import { MockUserValidationService } from './services/mock-user-validation.service';
import {
  PreDeliveryDecision,
  PreDeliveryRejectionReason,
} from './types/pre-delivery-decision.type';

@Injectable()
export class PreDeliveryOrchestratorService {
  private readonly logger = new Logger(PreDeliveryOrchestratorService.name);

  constructor(
    private readonly mockUserValidationService: MockUserValidationService,
    private readonly deliveryRoutingService: DeliveryRoutingService,
  ) {}

  async handle(notification: NotificationModel): Promise<void> {
    this.logger.log(
      `[PRE-DELIVERY] Start notificationId=${notification.id} userId=${notification.userId} appId=${notification.appId} type=${notification.type}`,
    );

    const channelsDecision = this.validateChannels(notification);

    if (!channelsDecision.accepted) {
      this.reject(notification, channelsDecision);
      return;
    }

    const userDecision = this.mockUserValidationService.validate(notification);

    if (!userDecision.accepted) {
      this.reject(notification, userDecision);
      return;
    }

    await this.deliveryRoutingService.route(notification);

    this.logger.log(
      `[PRE-DELIVERY] Completed notificationId=${notification.id}`,
    );
  }

  private validateChannels(notification: NotificationModel): PreDeliveryDecision {
    if (
      !Array.isArray(notification.channels) ||
      notification.channels.length === 0
    ) {
      return {
        accepted: false,
        reason: PreDeliveryRejectionReason.NO_CHANNELS,
        message: 'Notification has no delivery channels',
      };
    }

    this.logger.log(
      `[PRE-DELIVERY] Channels validation passed channels=${notification.channels.join(
        ',',
      )}`,
    );

    return {
      accepted: true,
    };
  }

  private reject(
    notification: NotificationModel,
    decision: PreDeliveryDecision,
  ): void {
    this.logger.warn(
      `[PRE-DELIVERY] Rejected notificationId=${notification.id} reason=${decision.reason} message=${decision.message ?? 'none'}`,
    );
  }
}