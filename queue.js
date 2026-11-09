import { Queue } from 'bullmq';
import IORedis from 'ioredis';

export const redisConnection = new IORedis({
  host: '127.0.0.1',
  port: 6379,
  maxRetriesPerRequest: null, // Required by BullMQ
});

export const extractionQueue = new Queue('ai-extraction-queue', {
  connection: redisConnection,
  defaultJobOptions: {
    attempts: 3, // Auto-retry up to 3 times if LLM outputs invalid JSON
    backoff: {
      type: 'exponential',
      delay: 1000, // Wait 1s, then 2s, then 4s between retries
    },
    removeOnComplete: 100, // Keep last 100 completed jobs for monitoring
    removeOnFail: 50,
  },
});
