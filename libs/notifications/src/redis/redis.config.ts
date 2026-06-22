export interface RedisLibraryConfig {
  host: string;
  port: number;
  password?: string;
  keyPrefix: string;
}

function getNumberEnv(name: string, defaultValue: number): number {
  const value = process.env[name];

  if (!value) {
    return defaultValue;
  }

  const parsedValue = Number(value);

  return Number.isNaN(parsedValue) ? defaultValue : parsedValue;
}

export function getRedisLibraryConfig(): RedisLibraryConfig {
  const password = process.env.REDIS_PASSWORD;

  return {
    host: process.env.REDIS_HOST ?? 'localhost',
    port: getNumberEnv('REDIS_PORT', 6379),
    password: password && password.length > 0 ? password : undefined,
    keyPrefix: process.env.REDIS_KEY_PREFIX ?? 'notification-lib:',
  };
}