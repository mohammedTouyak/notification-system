export const KafkaTopics = {
  NOTIFICATIONS: process.env.KAFKA_TOPIC_NOTIFICATIONS ?? 'notifications',
  NOTIFICATIONS_IN_APP:
    process.env.KAFKA_TOPIC_NOTIFICATIONS_IN_APP ?? 'notifications.in-app',
  NOTIFICATIONS_EMAIL:
    process.env.KAFKA_TOPIC_NOTIFICATIONS_EMAIL ?? 'notifications.email',
} as const;