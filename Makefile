.PHONY: build run start stop clean test

build:
	@mkdir -p bin
	go build -o bin/raft-node .

start: build
	@./scripts/start-cluster.sh

stop:
	@./scripts/stop-cluster.sh

clean:
	@./scripts/stop-cluster.sh 2>/dev/null || true
	rm -rf bin/ data/ logs/

test:
	go test ./... -v
