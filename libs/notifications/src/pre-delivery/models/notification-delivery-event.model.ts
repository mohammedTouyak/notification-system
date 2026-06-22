import { NotificationChannel } from '../../enums/notification-channel.enum';
import { NotificationStatus } from '../../enums/notification-status.enum';

export interface NotificationDeliveryEvent {
  eventId: string;
  notificationId: string;
  userId: string;
  appId: string;
  type: string;
  channel: NotificationChannel;
  templateKey: string;
  data: Record<string, unknown>;
  status: NotificationStatus;
  createdAt: string;
  routedAt: string;
}