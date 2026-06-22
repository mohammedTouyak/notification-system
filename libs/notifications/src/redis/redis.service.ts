import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import Redis from 'ioredis';
import { getRedisLibraryConfig, RedisLibraryConfig } from './redis.config';
/*
  * Role of this service:

  1. Create the Redis connection
  2. Connect Redis at NestJS startup
  3. Disconnect Redis properly at shutdown
  4. Expose useful methods:

  - incrementWithExpire
  - addToSortedSetWithTtl
  - getSortedSetValues
  - delete
  - ttl

  * So the other services don't communicate directly with ioredis; it uses RedisService.

  *Architecture :
  ```
    DigestPolicyService
          |
            v
    RedisService
            |
            v
    Redis

    BombardmentPolicyService
            |
            v
    RedisService
            |
            v
    Redis
  ```

*/

@Injectable()
export class RedisService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(RedisService.name);
  private readonly config: RedisLibraryConfig;

  private client?: Redis;
  private ready = false;

  constructor() {
    this.config = getRedisLibraryConfig();
  }

  async onModuleInit(): Promise<void> {
    this.client = new Redis({
      host: this.config.host,
      port: this.config.port,
      password: this.config.password,
      keyPrefix: this.config.keyPrefix,
      lazyConnect: true,
      retryStrategy: (times) => Math.min(times * 100, 3000),
    });

    this.client.on('connect', () => {
      this.logger.log('[REDIS] Connected');
    });

    this.client.on('ready', () => {
      this.ready = true;
      this.logger.log('[REDIS] Ready');
    });

    this.client.on('error', (error) => {
      this.ready = false;
      this.logger.error(`[REDIS] Error reason=${error.message}`);
    });

    this.client.on('close', () => {
      this.ready = false;
      this.logger.warn('[REDIS] Connection closed');
    });

    this.logger.log(
      `[REDIS] Connecting host=${this.config.host} port=${this.config.port}`,
    );

    await this.client.connect();
  }

  async onModuleDestroy(): Promise<void> {
    if (!this.client) {
      return;
    }

    this.logger.log('[REDIS] Disconnecting...');

    await this.client.quit();

    this.ready = false;

    this.logger.log('[REDIS] Disconnected');
  }

  isReady(): boolean {
    return this.ready;
  }

  async incrementWithExpire(key: string, ttlSeconds: number): Promise<number> {
    const client = this.getClient();

    // Lua script to increment the key and set expire if it's the first increment
    // This ensures atomicity of the operations: INCR + EXPIRE in the same step
    // If you make two separate commands and the application crashes between the two, you can have a key without expiration.
    const script = `
      local current = redis.call("INCR", KEYS[1])
      if current == 1 then
        redis.call("EXPIRE", KEYS[1], ARGV[1])
      end
      return current
    `;

    const result = await client.eval(script, 1, key, String(ttlSeconds));

    return Number(result);
  }

  async addToSortedSetWithTtl<T>(
    key: string,
    score: number,
    value: T,
    ttlSeconds: number,
  ): Promise<void> {
    const client = this.getClient();

    const serializedValue = JSON.stringify(value);

    await client.zadd(key, score, serializedValue);
    await client.expire(key, ttlSeconds);
  }

  async getSortedSetValues<T>(
    key: string,
    start = 0,
    stop = -1,
  ): Promise<T[]> {
    const values = await this.getClient().zrange(key, start, stop);

    return values.map((value) => JSON.parse(value) as T);
  }

  async delete(key: string): Promise<void> {
    await this.getClient().del(key);
  }

  async ttl(key: string): Promise<number> {
    return this.getClient().ttl(key);
  }

  private getClient(): Redis {
    if (!this.client || !this.ready) {
      throw new Error('[REDIS] Redis is not ready');
    }

    return this.client;
  }
}