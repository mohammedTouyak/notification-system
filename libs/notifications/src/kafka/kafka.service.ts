import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';


// # Role

// KafkaService is responsible for all communication with Kafka.

// It does 3 things:

// Create the Kafka client
// Connect the producer at NestJS startup
// Publish messages using emit(topic, payload)

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);

  private readonly kafka: Kafka;
  private readonly producer: Producer;

  constructor() {
    const clientId = process.env.KAFKA_CLIENT_ID ?? 'notification-api';

    const brokers = (
      process.env.KAFKA_BROKERS ?? 'localhost:9092'
    ).split(',');

    this.kafka = new Kafka({
      clientId,
      brokers,
    });

    this.producer = this.kafka.producer();
  }

  async onModuleInit(): Promise<void> {
    this.logger.log('[KAFKA PRODUCER] Connecting to Kafka...');

    await this.producer.connect();

    this.logger.log('[KAFKA PRODUCER] Connected');
  }

  async onModuleDestroy(): Promise<void> {
    this.logger.log('[KAFKA PRODUCER] Disconnecting from Kafka...');

    await this.producer.disconnect();

    this.logger.log('[KAFKA PRODUCER] Disconnected');
  }

  async emit(topic: string, payload: unknown, key?: string): Promise<void> {
    this.logger.log(`[KAFKA PUBLISH] topic=${topic}`);

    await this.producer.send({
      topic,
      messages: [
        {
          key,
          value: JSON.stringify(payload), // Kafka transports messages as bytes/string, so we need to serialize the payload to string.
        },
      ],
    });

    this.logger.log(`[KAFKA PUBLISHED] topic=${topic}`);
  }
}