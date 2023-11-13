NO_COLOR=\033[0m
OK_COLOR=\033[32;01m
ERROR_COLOR=\033[31;01m
WARN_COLOR=\033[33;01m

CLIENT_NAME=client
SERVER_NAME=server
ECS_NAME=ecs

.PHONY: all format build
all: format build

build: test build-client build-server build-ecs

run-client:
	@echo -e "$(OK_COLOR)==> Running $(CLIENT_NAME)...$(NO_COLOR)"
	@go run cmd/client/main.go

run-quorum-client:
	@echo -e "$(OK_COLOR)==> Running $(CLIENT_NAME)...$(NO_COLOR)"
	@go run cmd/quorum-client/main.go

run-server:
	@echo -e "$(OK_COLOR)==> Running $(SERVER_NAME)...$(NO_COLOR)"
	@go run cmd/server/*.go -b=localhost:1235 -a=localhost -p=8080

run-server-2:
	@echo -e "$(OK_COLOR)==> Running $(SERVER_NAME)...$(NO_COLOR)"
	@mkdir -p ./db-data/server2
	@go run cmd/server/*.go -b=localhost:1235 -a=localhost -p=8081 -d=./db-data/server2

run-server-3:
	@echo -e "$(OK_COLOR)==> Running $(SERVER_NAME)...$(NO_COLOR)"
	@mkdir -p ./db-data/server3
	@go run cmd/server/*.go -b=localhost:1235 -a=localhost -p=8082 -d=./db-data/server3

run-server-4:
	@echo -e "$(OK_COLOR)==> Running $(SERVER_NAME)...$(NO_COLOR)"
	@mkdir -p ./db-data/server4
	@go run cmd/server/*.go -b=localhost:1235 -a=localhost -p=8083 -d=./db-data/server4

run-ecs:
	@echo -e "$(OK_COLOR)==> Running $(ECS_NAME)...$(NO_COLOR)"
	@go run cmd/ecs/*.go -p=1235 -k=1236

build-client: test
	@echo -e "$(OK_COLOR)==> Building $(CLIENT_NAME)...$(NO_COLOR)"
	@go build -o bin/$(CLIENT_NAME) ./cmd/client
 
build-server: test
	@echo -e "$(OK_COLOR)==> Building $(SERVER_NAME)...$(NO_COLOR)"
	@go build -o bin/$(SERVER_NAME) ./cmd/server

build-ecs: test
	@echo -e "$(OK_COLOR)==> Building $(ECS_NAME)...$(NO_COLOR)"
	@go build -o bin/$(ECS_NAME) ./cmd/ecs

compile-client:
	@echo -e "$(OK_COLOR)==> Compiling $(CLIENT_NAME) for Linux x86-64...$(NO_COLOR)"
	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -o bin/$(CLIENT_NAME)-linux-amd64 cmd/client/main.go

compile-server:
	@echo -e "$(OK_COLOR)==> Compiling $(SERVER_NAME) for Linux x86-64...$(NO_COLOR)"
	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -o bin/$(SERVER_NAME)-linux-amd64 cmd/server/main.go

compile-ecs:
	@echo -e "$(OK_COLOR)==> Compiling $(ECS_NAME) for Linux x86-64...$(NO_COLOR)"
	@GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-s -w" -o bin/$(ECS_NAME)-linux-amd64 cmd/ecs/main.go

test: lint
	@echo -e "$(OK_COLOR)==> Testing $(SERVICE_NAME)...$(NO_COLOR)"
	@go test ./...

lint: 
	@echo -e "$(OK_COLOR)==> Linting $(SERVICE_NAME)...$(NO_COLOR)"
	@golangci-lint run

format:
	@echo -e "$(OK_COLOR)==> Formatting $(SERVICE_NAME)...$(NO_COLOR)"
	@go fmt ./...

clean-data:
	@echo -e "$(OK_COLOR)==> Cleaning data...$(NO_COLOR)"
	@rm -rf ./db-data/*

# Helm

lint-chart:
	@echo -e "$(OK_COLOR)==> Linting helm chart of $(SERVICE_NAME)... $(NO_COLOR)"
	@helm lint -f ./chart/values.yaml -f ./chart/values-develop.yaml ./chart
	@helm lint -f ./chart/values.yaml -f ./chart/values-production.yaml ./chart

render-chart:
	@echo -e "$(OK_COLOR)==> Rendering helm chart of $(SERVICE_NAME)... $(NO_COLOR)"
	@helm template --output-dir=.chart.rendered -f ./chart/values.yaml -f ./chart/values-develop.yaml ./chart
