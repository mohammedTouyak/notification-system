import { NotificationChannel } from '../enums/notification-channel.enum';
import { NotificationStatus } from '../enums/notification-status.enum';

export class NotificationModel {
  id: string;
  userId: string;
  appId: string;
  type: string;
  channels: NotificationChannel[];
  data: Record<string, unknown>;
  status: NotificationStatus;
  createdAt: Date;

  constructor(params: {
    id: string;
    userId: string;
    appId: string;
    type: string;
    channels: NotificationChannel[];
    data: Record<string, unknown>;
    status?: NotificationStatus;
    createdAt?: Date;
  }) {
    this.id = params.id;
    this.userId = params.userId;
    this.appId = params.appId;
    this.type = params.type;
    this.channels = params.channels;
    this.data = params.data;
    this.status = params.status ?? NotificationStatus.PENDING;
    this.createdAt = params.createdAt ?? new Date();
  }
}