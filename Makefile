-include .env
export

POSTGRES_USER ?= <YOUR-USER>
POSTGRES_PASSWORD ?= <YOUR-PASSWORD>
POSTGRES_HOST ?= localhost
POSTGRES_PORT ?= <YOUR-PORT>
POSTGRES_DB ?= <YOUR_DB_NAME>

# DSN строка для утилиты migrate
MIGRATE_DSN := postgres://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)?sslmode=disable
#Запуск приложения
run:
	go run cmd/api/main.go
#Запуск Докер-контейнера
d-up:
	docker compose up -d
#Остановка Докер-контейнера
d-down:
	docker compose down
#Создать таблицу make migrate-create name=add_user_table
migrate-create:
	migrate create -ext sql -dir migrations -seq $(name)
#Накат миграций
migrate-up:
	migrate -path migrations -database '$(MIGRATE_DSN)' up
#Откат миграций
migrate-down:
	migrate -path migrations -database '$(MIGRATE_DSN)' down 1
#Откат на версию(или если зависло) make migrate-force version=1
migrate-force:
	migrate -path migrations -database '$(MIGRATE_DSN)' force $(version)
#Запуск тестов
test:
	go test -v ./...
kafka-check:
	docker exec -it event-dispatcher-kafka /opt/kafka/bin/kafka-console-consumer.sh \
  	--bootstrap-server localhost:9092 \
  	--topic notifications_dlq \
  	--from-beginning