BIN=bin/control-panel-service
CONFIG?=./config.yaml
PID_FILE=.run.pid
PROTO_FILE=proto/control_panel_service.proto
PROTO_OUT=gen/controlpanelv1
MARKETDATA_PROTO_FILE=proto/marketdata_service.proto
MARKETDATA_PROTO_OUT=gen/marketdatav1

.PHONY: proto tidy test build dev start stop clean ensure-db

proto:
	mkdir -p $(PROTO_OUT) $(MARKETDATA_PROTO_OUT)
	PATH="$(HOME)/go/bin:$$PATH" protoc -I proto -I ../strategy-service/proto \
		--go_out=$(PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILE)
	PATH="$(HOME)/go/bin:$$PATH" protoc -I proto \
		--go_out=$(MARKETDATA_PROTO_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(MARKETDATA_PROTO_OUT) --go-grpc_opt=paths=source_relative \
		$(MARKETDATA_PROTO_FILE)

tidy:
	go mod tidy

test:
	go test ./...

ensure-db:
	go run ./cmd/ensure-control-panel-db

build:
	mkdir -p bin
	go build -o $(BIN) ./cmd/control-panel-service

dev:
	go run ./cmd/control-panel-service -config $(CONFIG)

start: build
	mkdir -p logs
	python3 -c 'import subprocess; out=open("logs/control-panel-service.out","ab",buffering=0); p=subprocess.Popen(["./$(BIN)","-config","$(CONFIG)"], stdout=out, stderr=subprocess.STDOUT, start_new_session=True, close_fds=True); open("$(PID_FILE)","w").write(str(p.pid)+"\n")'
	@echo "✓ control-panel-service started (pid=$$(cat $(PID_FILE))), logs at control-panel-service/logs/control-panel-service.out"

stop:
	@if [ -f $(PID_FILE) ]; then \
		pid=$$(cat $(PID_FILE)); \
		kill $$pid 2>/dev/null || true; \
		for _ in 1 2 3 4 5 6 7 8 9 10 11 12 13 14 15 16 17 18 19 20; do \
			kill -0 $$pid 2>/dev/null || break; \
			sleep 0.2; \
		done; \
		if kill -0 $$pid 2>/dev/null; then kill -KILL $$pid 2>/dev/null || true; fi; \
		rm -f $(PID_FILE); \
		echo "✓ control-panel-service stopped"; \
	else \
		echo "(no $(PID_FILE), nothing to stop)"; \
	fi

clean:
	rm -rf bin $(PID_FILE)
