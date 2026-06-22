import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { Kafka, Producer } from 'kafkajs';
import { getKafkaLibraryConfig, KafkaLibraryConfig } from './kafka.config';

@Injectable()
export class KafkaService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaService.name);

  private readonly config: KafkaLibraryConfig;

  private kafka?: Kafka;
  private producer?: Producer;

  private ready = false;
  private isConnecting = false;
  private isShuttingDown = false;

  private reconnectionTimer: NodeJS.Timeout | null = null;

  constructor() {
    this.config = getKafkaLibraryConfig();
  }

  async onModuleInit(): Promise<void> {
    if (this.config.connectInBackground) {
      this.initializeInBackground();
      return;
    }

    await this.initialize();
  }

  async onModuleDestroy(): Promise<void> {
    this.isShuttingDown = true;

    if (this.reconnectionTimer) {
      clearTimeout(this.reconnectionTimer);
      this.reconnectionTimer = null;
    }

    if (!this.producer) {
      return;
    }

    this.logger.log('[KAFKA PRODUCER] Disconnecting from Kafka...');

    await this.producer.disconnect();

    this.ready = false;

    this.logger.log('[KAFKA PRODUCER] Disconnected');
  }

  async emit<TPayload>(
    topic: string,
    payload: TPayload,
    key?: string,
    headers?: Record<string, string>,
  ): Promise<void> {
    if (!this.ready || !this.producer) {
      throw new Error(
        `[KAFKA PRODUCER] Kafka is not ready. Cannot publish topic=${topic}`,
      );
    }

    const value = JSON.stringify(payload);

    this.logger.log(`[KAFKA PUBLISH] topic=${topic} key=${key ?? 'none'}`);

    await this.producer.send({
      topic,
      messages: [
        {
          key,
          value,
          headers,
        },
      ],
    });

    this.logger.log(`[KAFKA PUBLISHED] topic=${topic} key=${key ?? 'none'}`);
  }

  isReady(): boolean {
    return this.ready;
  }

  private initializeInBackground(): void {
    setTimeout(() => {
      void this.initialize().catch((error) => {
        this.ready = false;

        this.logger.error(
          `[KAFKA PRODUCER] Initial connection failed. Application will continue. reason=${this.formatError(error)}`,
        );

        this.scheduleReconnection();
      });
    }, 0);
  }

  private async initialize(): Promise<void> {
    if (this.isConnecting || this.isShuttingDown) {
      return;
    }

    this.isConnecting = true;

    try {
      this.logger.log('[KAFKA PRODUCER] Initializing Kafka producer...');
      this.logger.log(
        `[KAFKA CONFIG] brokers=${this.config.brokers.join(',')} ssl=${this.config.ssl}`,
      );

      this.kafka = new Kafka({
        clientId: this.config.clientId,
        brokers: this.config.brokers,
        ssl: this.config.ssl,
      });

      this.producer = this.kafka.producer();

      this.registerProducerEvents(this.producer);

      this.logger.log('[KAFKA PRODUCER] Connecting to Kafka...');

      await this.producer.connect();

      this.ready = true;

      this.logger.log('[KAFKA PRODUCER] Connected and ready');
    } catch (error) {
      this.ready = false;
      throw error;
    } finally {
      this.isConnecting = false;
    }
  }

  private registerProducerEvents(producer: Producer): void {
    producer.on(producer.events.DISCONNECT, () => {
      if (this.isShuttingDown) {
        return;
      }

      this.ready = false;

      this.logger.error('[KAFKA PRODUCER] Disconnected unexpectedly');

      this.scheduleReconnection();
    });
  }

  private scheduleReconnection(): void {
    if (this.isShuttingDown || this.isConnecting || this.reconnectionTimer) {
      return;
    }

    this.logger.warn(
      `[KAFKA PRODUCER] Scheduling reconnection in ${
        this.config.reconnectionDelayMs / 1000
      } seconds`,
    );

    this.reconnectionTimer = setTimeout(() => {
      this.reconnectionTimer = null;

      void this.initialize().catch((error) => {
        this.ready = false;

        this.logger.error(
          `[KAFKA PRODUCER] Reconnection failed. reason=${this.formatError(
            error,
          )}`,
        );

        this.scheduleReconnection();
      });
    }, this.config.reconnectionDelayMs);
  }

  private formatError(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    return String(error);
  }
}