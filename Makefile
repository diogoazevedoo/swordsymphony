build:
	go build -o bin/api ./cmd/api

run:
	go run ./cmd/api

test:
	go test -v ./...

migrate:
	go run ./cmd/migrate -config=.env -migrations=./internal/migrations