export interface KafkaLibraryConfig {
  clientId: string;
  brokers: string[];
  ssl: boolean;

  connectInBackground: boolean;
  reconnectionDelayMs: number;

  consumerGroupId: string;
  consumerFromBeginning: boolean;
}

function getBooleanEnv(name: string, defaultValue: boolean): boolean {
  const value = process.env[name];

  if (value === undefined) {
    return defaultValue;
  }

  return value === 'true';
}

function getNumberEnv(name: string, defaultValue: number): number {
  const value = process.env[name];

  if (!value) {
    return defaultValue;
  }

  const parsedValue = Number(value);

  return Number.isNaN(parsedValue) ? defaultValue : parsedValue;
}

export function getKafkaLibraryConfig(): KafkaLibraryConfig {
  return {
    clientId: process.env.KAFKA_CLIENT_ID ?? 'notification-library',

    brokers: (process.env.KAFKA_BROKERS ?? 'localhost:9092')
      .split(',')
      .map((broker) => broker.trim())
      .filter(Boolean),

    ssl: getBooleanEnv('KAFKA_SSL_ENABLED', false),

    connectInBackground: getBooleanEnv('KAFKA_CONNECT_IN_BACKGROUND', true),

    reconnectionDelayMs: getNumberEnv(
      'KAFKA_RECONNECTION_DELAY_MS',
      300_000,
    ),

    consumerGroupId:
      process.env.KAFKA_CONSUMER_GROUP ?? 'notification-consumer-group',

    consumerFromBeginning: getBooleanEnv(
      'KAFKA_CONSUMER_FROM_BEGINNING',
      false,
    ),
  };
}