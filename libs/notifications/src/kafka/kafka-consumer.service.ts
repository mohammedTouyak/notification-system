import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import {
  Consumer,
  EachMessagePayload,
  Kafka,
  KafkaMessage,
} from 'kafkajs';
import { getKafkaLibraryConfig, KafkaLibraryConfig } from './kafka.config';

export interface KafkaMessageMetadata {
  topic: string;
  partition: number;
  offset: string;
  key?: string;
  timestamp?: string;
  headers?: Record<string, string>;
}

export type KafkaMessageHandler<T = unknown> = (
  payload: T,
  metadata: KafkaMessageMetadata,
) => Promise<void> | void;

@Injectable()
export class KafkaConsumerService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(KafkaConsumerService.name);

  private readonly config: KafkaLibraryConfig;

  private kafka?: Kafka;
  private consumer?: Consumer;

  private readonly handlers = new Map<string, KafkaMessageHandler>();
  private readonly topics = new Set<string>();
  private readonly subscribedTopics = new Set<string>();

  private ready = false;
  private isConnecting = false;
  private isRunning = false;
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

    if (!this.consumer) {
      return;
    }

    this.logger.log('[KAFKA CONSUMER] Disconnecting from Kafka...');

    await this.consumer.disconnect();

    this.ready = false;
    this.isRunning = false;
    this.subscribedTopics.clear();

    this.logger.log('[KAFKA CONSUMER] Disconnected');
  }

  async consume<T = unknown>(
    topic: string,
    handler: KafkaMessageHandler<T>,
  ): Promise<void> {
    this.handlers.set(topic, handler as KafkaMessageHandler);
    this.topics.add(topic);

    this.logger.log(
      `[KAFKA CONSUMER REGISTER] topic=${topic} group=${this.config.consumerGroupId}`,
    );

    if (!this.ready) {
      if (this.config.connectInBackground) {
        this.initializeInBackground();
        return;
      }

      await this.initialize();
      return;
    }

    await this.subscribeAndRunIfNeeded();
  }

  isReady(): boolean {
    return this.ready;
  }

  private initializeInBackground(): void {
    if (this.ready || this.isConnecting || this.reconnectionTimer) {
      return;
    }

    setTimeout(() => {
      void this.initialize().catch((error) => {
        this.ready = false;
        this.isRunning = false;

        this.logger.error(
          `[KAFKA CONSUMER] Initial connection failed. Application will continue. reason=${this.formatError(
            error,
          )}`,
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
      await this.disconnectCurrentConsumerSafely();

      this.logger.log('[KAFKA CONSUMER] Initializing Kafka consumer...');
      this.logger.log(
        `[KAFKA CONFIG] brokers=${this.config.brokers.join(',')} ssl=${
          this.config.ssl
        } group=${this.config.consumerGroupId}`,
      );

      this.kafka = new Kafka({
        clientId: `${this.config.clientId}-consumer`,
        brokers: this.config.brokers,
        ssl: this.config.ssl,
      });

      this.consumer = this.kafka.consumer({
        groupId: this.config.consumerGroupId,
      });

      this.registerConsumerEvents(this.consumer);

      this.logger.log('[KAFKA CONSUMER] Connecting to Kafka...');

      await this.consumer.connect();

      this.ready = true;

      this.logger.log('[KAFKA CONSUMER] Connected and ready');

      await this.subscribeAndRunIfNeeded();
    } catch (error) {
      this.ready = false;
      this.isRunning = false;
      throw error;
    } finally {
      this.isConnecting = false;
    }
  }

  private async subscribeAndRunIfNeeded(): Promise<void> {
    if (!this.consumer || !this.ready) {
      return;
    }

    if (this.topics.size === 0) {
      this.logger.log('[KAFKA CONSUMER] No topics registered yet');
      return;
    }

    for (const topic of this.topics) {
      if (this.subscribedTopics.has(topic)) {
        continue;
      }

      await this.consumer.subscribe({
        topic,
        fromBeginning: this.config.consumerFromBeginning,
      });

      this.subscribedTopics.add(topic);

      this.logger.log(
        `[KAFKA SUBSCRIBE] topic=${topic} group=${this.config.consumerGroupId} fromBeginning=${this.config.consumerFromBeginning}`,
      );
    }

    if (!this.isRunning) {
      await this.run();
    }
  }

  private async run(): Promise<void> {
    if (!this.consumer) {
      return;
    }

    this.isRunning = true;

    this.logger.log('[KAFKA CONSUMER] Starting message loop');

    await this.consumer.run({
      eachMessage: async (payload: EachMessagePayload) => {
        await this.handleMessage(payload);
      },
    });

    this.logger.log('[KAFKA CONSUMER] Message loop started');
  }

  private async handleMessage({
    topic,
    partition,
    message,
  }: EachMessagePayload): Promise<void> {
    const metadata: KafkaMessageMetadata = {
      topic,
      partition,
      offset: message.offset,
      key: message.key?.toString(),
      timestamp: message.timestamp,
      headers: this.parseHeaders(message.headers),
    };

    const handler = this.handlers.get(topic);

    if (!handler) {
      this.logger.warn(`[KAFKA RECEIVED] No handler for topic=${topic}`);
      return;
    }

    if (!message.value) {
      this.logger.warn(
        `[KAFKA RECEIVED] Empty message topic=${topic} partition=${partition} offset=${message.offset}`,
      );
      return;
    }

    const parsedPayload = this.parseMessageValue(message.value, metadata);

    if (parsedPayload === null) {
      return;
    }

    this.logger.log(
      `[KAFKA RECEIVED] topic=${topic} partition=${partition} offset=${message.offset} key=${
        metadata.key ?? 'none'
      }`,
    );

    try {
      await handler(parsedPayload, metadata);
    } catch (error) {
      this.logger.error(
        `[KAFKA HANDLER ERROR] topic=${topic} partition=${partition} offset=${message.offset} reason=${this.formatError(
          error,
        )}`,
      );

      throw error;
    }
  }

  private parseMessageValue<T>(
    value: Buffer,
    metadata: KafkaMessageMetadata,
  ): T | null {
    try {
      return JSON.parse(value.toString()) as T;
    } catch (error) {
      this.logger.error(
        `[KAFKA CONSUMER ERROR] Invalid JSON payload topic=${metadata.topic} partition=${metadata.partition} offset=${metadata.offset} reason=${this.formatError(
          error,
        )}`,
      );

      return null;
    }
  }

  private parseHeaders(
    headers?: KafkaMessage['headers'],
  ): Record<string, string> | undefined {
    if (!headers) {
      return undefined;
    }

    const parsedHeaders: Record<string, string> = {};

    for (const [key, value] of Object.entries(headers)) {
      if (value === undefined) {
        continue;
      }

      if (Array.isArray(value)) {
        parsedHeaders[key] = value
          .map((item) => (Buffer.isBuffer(item) ? item.toString() : String(item)))
          .join(',');
        continue;
      }

      parsedHeaders[key] = Buffer.isBuffer(value)
        ? value.toString()
        : String(value);
    }

    return Object.keys(parsedHeaders).length > 0 ? parsedHeaders : undefined;
  }

  private registerConsumerEvents(consumer: Consumer): void {
    consumer.on(consumer.events.DISCONNECT, () => {
      if (this.isShuttingDown) {
        return;
      }

      this.ready = false;
      this.isRunning = false;
      this.subscribedTopics.clear();

      this.logger.error('[KAFKA CONSUMER] Disconnected unexpectedly');

      this.scheduleReconnection();
    });

    consumer.on(consumer.events.CRASH, (event) => {
      if (this.isShuttingDown) {
        return;
      }

      this.ready = false;
      this.isRunning = false;
      this.subscribedTopics.clear();

      this.logger.error(
        `[KAFKA CONSUMER] Crashed. reason=${this.formatError(event)}`,
      );

      this.scheduleReconnection();
    });
  }

  private scheduleReconnection(): void {
    if (this.isShuttingDown || this.isConnecting || this.reconnectionTimer) {
      return;
    }

    this.logger.warn(
      `[KAFKA CONSUMER] Scheduling reconnection in ${
        this.config.reconnectionDelayMs / 1000
      } seconds`,
    );

    this.reconnectionTimer = setTimeout(() => {
      this.reconnectionTimer = null;

      void this.initialize().catch((error) => {
        this.ready = false;
        this.isRunning = false;

        this.logger.error(
          `[KAFKA CONSUMER] Reconnection failed. reason=${this.formatError(
            error,
          )}`,
        );

        this.scheduleReconnection();
      });
    }, this.config.reconnectionDelayMs);
  }

  private async disconnectCurrentConsumerSafely(): Promise<void> {
    if (!this.consumer) {
      return;
    }

    try {
      await this.consumer.disconnect();
    } catch (error) {
      this.logger.warn(
        `[KAFKA CONSUMER] Failed to disconnect previous consumer. reason=${this.formatError(
          error,
        )}`,
      );
    } finally {
      this.consumer = undefined;
      this.ready = false;
      this.isRunning = false;
      this.subscribedTopics.clear();
    }
  }

  private formatError(error: unknown): string {
    if (error instanceof Error) {
      return error.message;
    }

    return String(error);
  }
}