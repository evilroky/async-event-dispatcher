# Async-event-dispatcher
My first Pet-project on go
Высоконагруженный асинхронный сервис уведомлений на Go, построенный по принципам **Clean Architecture** с использованием **Apache Kafka, PostgreSQL и Redis**.

Сервис обеспечивает асинхронную обработку уведомлений, гарантированную доставку сообщений с поддержкой автоматических ретраев (**Exponential Backoff**) и обработку сбойных событий через **Dead Letter Queue (DLQ)**.

---

## Технологический стек

- **Language:** Go 1.22
- **HTTP Router:** `go-chi/chi/v5`
- **Message Broker:** Apache Kafka (`IBM/sarama`)
- **Database:** PostgreSQL (`jackc/pgx/v5`)
- **Cache:** Redis (`redis/go-redis/v9`)
- **Logging:** `log/slog`
- **Observability:** Jaeger / OpenTelemetry
- **Containerization:** Docker / Docker Compose
- **Database Migrations:** `golang-migrate`

---

## Архитектура приложения

```mermaid
flowchart TD
    Client([Client / Insomnia]) -->|POST /api/v1/notifications| API[HTTP API Handler]
    
    subgraph API_Layer [API & Service Layer]
        API -->|1. Check Template| TDB[(PostgreSQL)]
        API -->|2. Save Pending Status| NDB[(PostgreSQL)]
        API -->|3. Publish Event| Producer[Kafka Producer]
    end

    Producer -->|Topic: notifications| Kafka{{Apache Kafka}}

    subgraph Consumer_Layer [Async Worker / Consumer]
        Kafka -->|4. Consume Message| Worker[Consumer Group Worker]
        
        Worker -->|5. Process & Send| Service[Notification Service]
        
        Service -->|Success| SuccessStatus[Update Status: SENT]
        SuccessStatus --> NDB
        
        Service -->|Failure| RetryLoop{Retry Loop<br/>Max 3 Attempts}
        
        RetryLoop -->|Retry Success| SuccessStatus
        RetryLoop -->|Exhausted| FailStatus[Update Status: FAILED]
        FailStatus --> NDB
        
        RetryLoop -->|Exhausted| DLQProducer[DLQ Producer]
        DLQProducer -->|6. Publish Failed Event| DLQTopic{{Topic: notifications_dlq}}
    end
```

---

# Быстрый запуск

Для управления приложением, Docker-инфраструктурой, миграциями и тестами используется `Makefile`.

## 1. Требования

Перед запуском убедитесь, что установлены:

- Go 1.22+
- Docker
- Docker Compose
- Make
- `golang-migrate`

Проверить установленные версии:

```bash
go version
docker --version
docker compose version
make --version
migrate -version
```

---

# Конфигурация

Проект использует переменные окружения для подключения к PostgreSQL.

Создайте файл `.env` в корне проекта:

```env
POSTGRES_DB=<YOUR-DB-NAME>
POSTGRES_USER=<YOUR-USER>
POSTGRES_PASSWORD=<YOUR-PASSWORD>
POSTGRES_PORT=<YOUR-PORT>

ENV=local
HTTP_PORT=<YOUR-PORT-HTTP>

REDIS_HOST=<YOUR-HOST>
REDIS_PORT=<YOUR-PORT>
REDIS_PASSWORD=<YOUR-PASSWORD>

KAFKA_BROKERS=localhost:9092
KAFKA_TOPIC=<YOUR_TOPIC>
KAFKA_DLQ_TOPIC=<YOUR-KAFKA_DLQ_TOPIC>
```

`Makefile` автоматически загружает переменные из `.env`:

```makefile
-include .env
export
```

Если переменная не указана в `.env`, используются значения по умолчанию:

```text
POSTGRES_USER     = <YOUR-USER>
POSTGRES_PASSWORD = <YOUR-PASSWORD>
POSTGRES_HOST     = localhost
POSTGRES_PORT     = <YOUR-PORT>
POSTGRES_DB       = <YOUR_DB_NAME>
```

---

# Docker

## Запуск инфраструктуры

Запустить Docker Compose в фоновом режиме:

```bash
make d-up
```

Команда выполняет:

```bash
docker compose up -d
```

В зависимости от конфигурации Docker Compose будут запущены необходимые сервисы проекта, например:

- PostgreSQL
- Kafka
- Redis
- Jaeger
- другие необходимые компоненты

## Остановка Docker

Остановить и удалить контейнеры:

```bash
make d-down
```

Команда выполняет:

```bash
docker compose down
```

---

# Запуск приложения

## Локальный запуск

Для запуска Go-приложения непосредственно на хост-машине:

```bash
make d-up
```

```bash
make run
```

Или напрямую:
```bash
docker-compose up --build
```

```bash
go run cmd/api/main.go
```

Перед локальным запуском убедитесь, что необходимые инфраструктурные сервисы запущены через Docker.

---

# Database Migrations

Для работы с миграциями используется `golang-migrate`.

## Создание новой миграции

Для создания новой миграции необходимо передать имя через переменную `name`.

Пример:

```bash
make migrate-create name=add_user_table
```

В результате в директории `migrations` будут созданы файлы вида:

```text
000001_add_user_table.up.sql
000001_add_user_table.down.sql
```

---

## Накат всех миграций

Применить все доступные миграции:

```bash
make migrate-up
```

Команда использует DSN:

```text
postgres://USER:PASSWORD@HOST:PORT/DB?sslmode=disable
```

DSN автоматически формируется в `Makefile`:

```makefile
MIGRATE_DSN := postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
```

---

## Откат последней миграции

Откатить последнюю применённую миграцию:

```bash
make migrate-down
```

Команда выполняет откат на одну версию:

```bash
migrate -path migrations -database '$(MIGRATE_DSN)' down 1
```

---

## Принудительная установка версии миграций

Если миграции зависли или состояние базы данных требует ручной коррекции, можно принудительно установить версию:

```bash
make migrate-force version=1
```

Например:

```bash
make migrate-force version=5
```

> `migrate-force` изменяет только версию, которую `golang-migrate` считает текущей. Команда не выполняет SQL-миграцию.

---

# Тестирование

Запустить все тесты проекта:

```bash
make test
```

Команда выполняет:

```bash
go test -v ./...
```

Для запуска конкретного пакета можно использовать стандартный Go:

```bash
go test -v ./path/to/package
```

---

#  API Endpoints

## 1. Health Check

Проверка доступности приложения:

```http
GET http://localhost:8080/health
```

### Response

```json
{
  "status": "ok"
}
```

---

## 2. Отправка уведомления

Создание нового уведомления:

```http
POST http://localhost:8080/api/v1/notifications
```

### Request Body

```json
{
  "user_id": "usr_12345",
  "template_code": "welcome_email",
  "payload": {
    "name": "Alex"
  }
}
```

### Response

HTTP Status:

```text
202 Accepted
```

Пример ответа:

```json
{
  "id": "c1fa5d1d-ddda-4824-aae1-d9d7fbfce024",
  "user_id": "usr_12345",
  "template_code": "welcome_email",
  "status": "pending",
  "created_at": "2026-09-02T21:41:10Z"
}
```

После создания уведомления оно сохраняется в PostgreSQL со статусом `pending`, после чего событие публикуется в Kafka.

Дальнейшая обработка выполняется асинхронным Kafka Consumer.

---

## 3. Получение статуса уведомления

Получить текущее состояние уведомления:

```http
GET http://localhost:8080/api/v1/notifications/{id}
```

Пример:

```http
GET http://localhost:8080/api/v1/notifications/c1fa5d1d-ddda-4824-aae1-d9d7fbfce024
```

---

# Обработка уведомлений

После создания уведомления система работает по следующему сценарию:

1. HTTP API принимает запрос.
2. Проверяется существование необходимого шаблона.
3. Уведомление сохраняется в PostgreSQL со статусом `pending`.
4. Событие публикуется в Kafka topic `notifications`.
5. Kafka Consumer получает сообщение.
6. Notification Service выполняет обработку уведомления.
7. При успешной обработке статус изменяется на `sent`.
8. При ошибке запускается механизм автоматического повторения.
9. После исчерпания максимального количества попыток уведомление получает статус `failed`.
10. Исходное сообщение отправляется в `notifications_dlq`.

---

# Retry Mechanism

Для обработки временных ошибок используется механизм автоматических повторных попыток.

Количество попыток:

```text
3
```

При каждой неудачной попытке используется **Exponential Backoff**, благодаря чему система не создаёт большое количество повторных запросов при временной недоступности внешнего сервиса.

Упрощённо схема выглядит следующим образом:

```text
Attempt 1
   ↓
Failure
   ↓
Backoff
   ↓
Attempt 2
   ↓
Failure
   ↓
Backoff
   ↓
Attempt 3
   ↓
Failure
   ↓
FAILED + DLQ
```

Если одна из повторных попыток завершается успешно:

```text
Attempt
   ↓
Success
   ↓
SENT
```

---

# Dead Letter Queue

Для сообщений, которые не удалось обработать после всех попыток, используется **Dead Letter Queue (DLQ)**.

Kafka topic:

```text
notifications_dlq
```

После исчерпания трёх попыток сообщение публикуется в этот topic для последующего анализа и ручной обработки.

---

# Проверка DLQ

Для просмотра сообщений из `notifications_dlq` используется:

```bash
make kafka-check
```

Команда выполняет:

```bash
docker exec -it event-dispatcher-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic notifications_dlq \
  --from-beginning
```

Consumer читает сообщения начиная с самого первого:

```text
--from-beginning
```

Это позволяет проверить все накопленные сообщения в DLQ.

---

# Команды из Makefile и их описание

| Команда | Описание |
|---|---|
| `make run` | Локальный запуск Go-приложения |
| `make d-up` | Запуск Docker Compose в фоновом режиме |
| `make d-down` | Остановка и удаление Docker-контейнеров |
| `make migrate-create name=<name>` | Создание новой SQL-миграции |
| `make migrate-up` | Применение всех доступных миграций |
| `make migrate-down` | Откат последней миграции |
| `make migrate-force version=<version>` | Принудительная установка версии миграции |
| `make test` | Запуск всех тестов |
| `make kafka-check` | Просмотр сообщений из `notifications_dlq` |

---

# Примеры основных команд

### Первый запуск проекта

```bash
# 1. Запустить инфраструктуру
make d-up

# 2. Применить миграции
make migrate-up

# 3. Запустить приложение
make run
```

### Разработка

```bash
# Запустить инфраструктуру
make d-up

# Запустить приложение
make run

# Запустить тесты
make test
```

### Работа с базой данных

```bash
# Создать миграцию
make migrate-create name=add_notification_table

# Применить миграции
make migrate-up

# Откатить последнюю миграцию
make migrate-down
```

### Проверка DLQ

```bash
make kafka-check
```

### Завершение работы

```bash
make d-down
```

---

# Основные статусы уведомления

| Статус | Описание |
|---|---|
| `pending` | Уведомление создано и ожидает обработки |
| `sent` | Уведомление успешно обработано |
| `failed` | Все попытки обработки завершились ошибкой |

---

# Observability

Для мониторинга и трассировки используется **OpenTelemetry** совместно с **Jaeger**.

Это позволяет отслеживать путь уведомления через основные компоненты системы:

```text
HTTP API
   ↓
PostgreSQL
   ↓
Kafka Producer
   ↓
Kafka
   ↓
Kafka Consumer
   ↓
Notification Service
   ↓
PostgreSQL
```

Такой подход позволяет находить узкие места и диагностировать ошибки при асинхронной обработке сообщений.

---

Спасибо за прочтение! Это был увлекательный проект. Дальше - больше.