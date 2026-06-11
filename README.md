# Personal Notification System

## Project Goal

This project is a personal educational project built to understand how a professional distributed notification system works.

The goal is to learn, step by step, how to build a notification architecture using:

* NestJS monorepo
* Internal NestJS libraries
* REST API
* Kafka
* Kafka producers and consumers
* Pre-delivery pipeline
* IN-APP notifications
* Simulated EMAIL notifications
* Socket.IO
* Socket.IO rooms
* Redis Adapter
* Postgres
* Docker Compose
* Multi-instance API testing

The project reproduces a common professional problem:

```text
A user is connected to api-1,
but Kafka may give the notification processing to api-2.

Without Redis Adapter, api-2 cannot reach the socket connected to api-1.

With Redis Adapter, api-2 can emit the notification and api-1 can deliver it to the connected user.
```

---

## Target Architecture

```text
POST /api/notifications
        |
        v
SendNotificationController
        |
        v
SendNotificationUseCase
        |
        v
SendNotificationService
        |
        v
Kafka topic: notifications
        |
        v
NotificationConsumer
        |
        v
PreDeliveryOrchestratorService
        |
        +----------------------------+
        |                            |
        v                            v
Kafka topic:                 Kafka topic:
notifications.in-app         notifications.email
        |                            |
        v                            v
InAppNotificationConsumer    EmailNotificationConsumer
        |                            |
        v                            v
TemplateParserService        TemplateParserService
        |                            |
        v                            v
Postgres                     Email simulated logs
        |
        v
InAppNotificationGateway
        |
        v
Socket.IO room:
user:{userId}:app:{appId}
```

---

## Main Workflow

A client or another backend service sends a notification request:

```http
POST /api/notifications
```

Example body:

```json
{
  "userId": "55",
  "appId": "demo-app",
  "type": "APPOINTMENT_CREATED",
  "channels": ["IN_APP", "EMAIL"],
  "data": {
    "doctorName": "Dr Ahmed",
    "date": "2026-06-12",
    "time": "09:30"
  }
}
```

The system does not process everything inside the HTTP request.

Instead, it publishes a notification event to Kafka. Then Kafka consumers process the notification asynchronously.

---

## Business Workflow

```text
Notification requested
        |
        v
Validate minimal request data
        |
        v
Publish notification event
        |
        v
Apply pre-delivery rules
        |
        v
Choose delivery channels
        |
        v
Generate final message
        |
        v
Save delivery history
        |
        v
Deliver notification
```

---

## Technical Workflow

```text
REST API
= receives the notification request

UseCase
= represents the business scenario

Kafka
= transports notification events between backend steps

Consumer
= processes Kafka messages in the background

PreDeliveryOrchestrator
= applies business rules before delivery

TemplateParser
= transforms data into human-readable messages

Postgres
= stores notification history

Socket.IO
= sends real-time notification to the connected user

Socket.IO Room
= targets a specific user/application

Redis Adapter
= shares Socket.IO events between multiple API instances

Docker Compose
= runs the full local distributed environment
```

---

## Why Kafka?

Kafka is used for the backend notification workflow.

It allows the system to separate the HTTP request from the notification processing.

Without Kafka:

```text
POST /api/notifications
        |
        v
validate user
parse template
save database
send email
send websocket
return response
```

The request can become slow and difficult to maintain.

With Kafka:

```text
POST /api/notifications
        |
        v
publish event to Kafka
        |
        v
return fast HTTP response
        |
        v
process notification asynchronously
```

Kafka is useful here because it supports:

* asynchronous processing
* event-driven architecture
* consumer groups
* scalable workers
* retry and replay possibilities
* decoupling between services

---

## Why Socket.IO?

Socket.IO is used for real-time IN-APP notifications.

HTTP works like this:

```text
client sends request
server returns response
connection ends
```

But notifications need this behavior:

```text
server wants to push a message to the connected user immediately
```

Socket.IO keeps an active connection between the client and the server.

This allows the backend to emit:

```text
notification
```

to the connected user.

---

## Why Socket.IO Rooms?

Rooms allow the server to target a specific user.

Room format:

```text
user:{userId}:app:{appId}
```

Example:

```text
user:55:app:demo-app
```

Without rooms:

```text
server.emit("notification", payload)
```

All connected users may receive the notification.

With rooms:

```text
server.to("user:55:app:demo-app").emit("notification", payload)
```

Only the correct user in the correct application receives the notification.

---

## Why Redis Adapter?

Redis Adapter is used for Socket.IO multi-instance communication.

Problem:

```text
User connected to api-1
        |
        v
Room exists locally on api-1:
user:55:app:demo-app

Kafka gives notification to api-2
        |
        v
api-2 emits to the room

Without Redis Adapter:
api-2 does not know sockets connected to api-1
```

Solution:

```text
api-2 emits notification
        |
        v
Redis Adapter publishes the Socket.IO event
        |
        v
api-1 receives the event through Redis Adapter
        |
        v
api-1 finds the socket in its local room
        |
        v
user receives notification
```

Important note:

```text
Redis Adapter does not store Socket.IO rooms.

Rooms remain local to each API instance.

Redis is used as a Pub/Sub bridge between Socket.IO servers.
```

---

## Why Postgres?

Postgres is used to store notification history.

This is important because:

* the user may be offline
* the user must see old notifications later
* the system needs delivery history
* notifications can be marked as read
* debugging and auditing become easier

Redis is not the main database for this project.

Redis is used only for Socket.IO Redis Adapter and possibly temporary counters later.

---

## Target Folder Structure

```text
personal-notification-system/
├── apps/
│   └── api/
│       ├── src/
│       │   ├── main.ts
│       │   ├── app.module.ts
│       │   ├── debug/
│       │   │   └── debug-notifications.controller.ts
│       │   └── infrastructure/
│       │       └── websocket/
│       │           └── redis-io.adapter.ts
│       └── Dockerfile
│
├── libs/
│   └── notifications/
│       └── src/
│           ├── notifications.module.ts
│           ├── send-notification/
│           │   ├── send-notification.controller.ts
│           │   ├── send-notification.usecase.ts
│           │   └── send-notification.service.ts
│           ├── pre-delivery/
│           │   ├── notification.consumer.ts
│           │   └── pre-delivery-orchestrator.service.ts
│           ├── delivery/
│           │   ├── in-app-notification.consumer.ts
│           │   └── email-notification.consumer.ts
│           ├── gateway/
│           │   └── in-app-notification.gateway.ts
│           ├── template/
│           │   └── template-parser.service.ts
│           ├── persistence/
│           │   └── notification.repository.ts
│           ├── kafka/
│           │   ├── kafka.service.ts
│           │   └── kafka-consumer.service.ts
│           └── models/
│               └── notification.model.ts
│
├── docker-compose.yml
├── package.json
└── README.md
```

---

## Docker Compose Target

The local environment will contain:

```text
redis
kafka
postgres
api-1
api-2
```

Ports:

```text
api-1 -> localhost:4000
api-2 -> localhost:4001
```

Shared environment variables:

```env
CLIENT_ID=demo-app
WS_REDIS_ADAPTER_ENABLED=true
REDIS_HOST=redis
REDIS_PORT=6379
```

---

## Final Multi-Instance Test

Step 1:

Connect Postman Socket.IO to:

```text
ws://localhost:4000
```

With:

```text
userId=55
appId=demo-app
```

Expected log from api-1:

```text
[WS JOIN] room=user:55:app:demo-app
```

Step 2:

Call debug endpoint on api-2:

```http
POST http://localhost:4001/api/debug/notifications/in-app
```

Expected log from api-2:

```text
[WS NOTIFICATION] room=user:55:app:demo-app
```

Expected result:

```text
Postman connected to api-1 receives event: notification
```

This proves that Redis Adapter works.

---

## Interview Summary

This project demonstrates a distributed notification architecture.

The REST API receives a notification request, then Kafka is used to process the notification asynchronously. A pre-delivery pipeline applies business rules before routing the notification to specialized delivery topics such as IN-APP and EMAIL. Each delivery channel has its own consumer. The final message is generated using a template parser. IN-APP notifications are stored in Postgres and delivered in real time through Socket.IO rooms. Redis Adapter allows multiple NestJS instances to share Socket.IO events, which solves the problem where a user is connected to one API instance while the notification is processed by another instance.

---

## Interview Q&A

### Why use Kafka?

Kafka is used to decouple the HTTP request from the notification processing. It allows asynchronous processing, scalability and separation between workflow steps.

### Why not process the notification directly in the controller?

Because the controller would become too large and the HTTP request could become slow. A clean architecture separates the controller, use case, service, Kafka workflow, consumers and delivery logic.

### Why use Socket.IO?

Socket.IO allows the backend to push real-time notifications to connected clients.

### Why use rooms?

Rooms allow the backend to send a notification only to a specific user and application.

### Why use Redis Adapter?

Redis Adapter allows multiple Socket.IO servers to share events. This is required when the user is connected to api-1 but the notification is emitted from api-2.

### Does Redis store the rooms?

No. Rooms remain local to each Socket.IO server. Redis Adapter uses Redis Pub/Sub to propagate events between instances.

### Why use Postgres?

Postgres stores the notification history permanently. This allows users to see old notifications and allows the system to track delivery status.
