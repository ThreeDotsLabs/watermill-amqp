up:
	docker compose up -d

down:
	docker compose down

test:
	go test -parallel 20 ./...

test_v:
	go test -parallel 20 -v ./...

test_short:
	go test -parallel 20 ./... -short

test_race:
	go test ./... -short -race

test_stress:
	go test -tags=stress -parallel 30 -timeout=45m ./...

test_codecov: up wait
	go test -coverprofile=coverage.out -covermode=atomic ./...

test_reconnect:
	go test -tags=reconnect ./...

wait:
	go run github.com/ThreeDotsLabs/wait-for@latest localhost:5672
	go run github.com/ThreeDotsLabs/wait-for@latest localhost:15672

build:
	go build ./...

fmt:
	go fmt ./...
	goimports -l -w .

update_watermill:
	go get -u github.com/ThreeDotsLabs/watermill
	go mod tidy

	sed -i '\|go 1\.|d' go.mod
	go mod edit -fmt

