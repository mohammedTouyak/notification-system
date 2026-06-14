import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Consumer, EachMessagePayload, Kafka } from 'kafkajs';

export interface KafkaMessageMetadata {
  topic: string;
  partition: number;
  offset: string;
  key?: string;
}

export type KafkaMessageHandler<T = unknown> = (
  payload: T,
  metadata: KafkaMessageMetadata,
) => Promise<void> | void;

// C’est le service technique qui parle avec Kafka.

// Role :

// connecter consumer
// s’abonner à un topic
// lire les messages
// parser JSON
// appeler un callback métier

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);

  private readonly kafka: Kafka;
  private readonly consumer: Consumer;

  private readonly handlers = new Map<string, KafkaMessageHandler>();
  private isConnected = false;
  private isRunning = false;

  constructor() {
    const clientId = process.env.KAFKA_CLIENT_ID ?? 'notification-api';

    const brokers = (
      process.env.KAFKA_BROKERS ?? 'localhost:9092'
    ).split(',');

    const groupId =
      process.env.KAFKA_CONSUMER_GROUP ?? 'notification-consumer-group';

    this.kafka = new Kafka({
      clientId: `${clientId}-consumer`,
      brokers,
    });

    this.consumer = this.kafka.consumer({
      groupId,
    });
  }

  async onModuleInit(): Promise<void> {
    await this.connect();
  }

  async onModuleDestroy(): Promise<void> {
    if (!this.isConnected) {
      return;
    }

    this.logger.log('[KAFKA CONSUMER] Disconnecting from Kafka...');

    await this.consumer.disconnect();

    this.isConnected = false;

    this.logger.log('[KAFKA CONSUMER] Disconnected');
  }

  async consume<T = unknown>(
    topic: string,
    handler: KafkaMessageHandler<T>,
  ): Promise<void> {
    await this.connect();

    this.handlers.set(topic, handler as KafkaMessageHandler);

    await this.consumer.subscribe({ // Écoute le topic notifications.
      topic,
      fromBeginning: false, // Ne lis pas forcément tous les anciens messages depuis le début. Lis surtout les nouveaux messages.
    });

    this.logger.log(
      `[KAFKA SUBSCRIBE] topic=${topic} group=${
        process.env.KAFKA_CONSUMER_GROUP ?? 'notification-consumer-group'
      }`,
    );

    if (!this.isRunning) {
      await this.run();
    }
  }

  private async connect(): Promise<void> {
    if (this.isConnected) {
      return;
    }

    this.logger.log('[KAFKA CONSUMER] Connecting to Kafka...');

    await this.consumer.connect();

    this.isConnected = true;

    this.logger.log('[KAFKA CONSUMER] Connected');
  }

  private async run(): Promise<void> {
    this.isRunning = true;

    await this.consumer.run({ //Pour chaque message reçu depuis Kafka, exécute cette fonction.
      eachMessage: async (payload: EachMessagePayload) => {
        await this.handleMessage(payload);
      },
    });
  }

  private async handleMessage({
    topic,
    partition,
    message,
  }: EachMessagePayload): Promise<void> {
    const handler = this.handlers.get(topic);

    if (!handler) {
      this.logger.warn(`[KAFKA RECEIVED] No handler for topic=${topic}`);
      return;
    }

    if (!message.value) {
      this.logger.warn(`[KAFKA RECEIVED] Empty message topic=${topic}`);
      return;
    }

    try {
      const rawValue = message.value.toString();
      const parsedPayload = JSON.parse(rawValue);

      this.logger.log(
        `[KAFKA RECEIVED] topic=${topic} partition=${partition} offset=${message.offset}`,
      );

      await handler(parsedPayload, {
        topic,
        partition,
        offset: message.offset,
        key: message.key?.toString(),
      });
    } catch (error) {
      this.logger.error(
        `[KAFKA CONSUMER ERROR] Invalid message payload topic=${topic}`,
        error instanceof Error ? error.stack : String(error),
      );
    }
  }
}