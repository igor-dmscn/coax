# The fast checks run on every change; the slow ones need Docker, npm, or a lot
# of file descriptors, so they are separate targets rather than skipped tests
# nobody ever runs.
#
#   make            fmt, vet, race — what to run before every commit
#   make conformance autobahn, the Rails JS client, and Redis: the three
#                    external things this claims to be compatible with
#   make everything  all of it, including fuzzing and the load test

REDIS_IMAGE   ?= redis:7-alpine
REDIS_PORT    ?= 16379
REDIS_NAME    ?= coax-redis
FUZZTIME      ?= 30s
CABLE_CONNS   ?= 10000

.PHONY: default
default: fmt vet race

.PHONY: fmt
fmt:
	gofmt -l -w .

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test ./...

.PHONY: race
race:
	go test -race -count=1 ./...

.PHONY: bench
bench:
	go test ./... -run '^$$' -bench . -benchmem

# Every parser that reads bytes off a network gets fuzzed: WebSocket frames,
# RESP replies, and inbound JSON commands.
.PHONY: fuzz
fuzz:
	go test ./ws/ -run '^$$' -fuzz FuzzFrameParse -fuzztime $(FUZZTIME)
	go test ./coax/ -run '^$$' -fuzz FuzzDecodeCommand -fuzztime $(FUZZTIME)
	go test ./coax/redispubsub/ -run '^$$' -fuzz FuzzReadValue -fuzztime $(FUZZTIME)

# Autobahn's fuzzing client against our server, in Docker. Writes a summary to
# ws/testdata/autobahn/summary.json, which is committed as evidence.
.PHONY: autobahn
autobahn:
	WS_AUTOBAHN=1 go test ./ws/ -run TestAutobahn -count=1 -v -timeout 15m

# The unmodified @rails/actioncable client, under Node. Needs npm and network on
# first run to install it.
.PHONY: jsclient
jsclient:
	CABLE_JS=1 go test ./coax/ -run TestRailsJSClient -count=1 -v -timeout 5m

# A real Redis in Docker, started and stopped around the tests.
.PHONY: redis
redis:
	docker run --rm -d --name $(REDIS_NAME) -p $(REDIS_PORT):6379 $(REDIS_IMAGE)
	sleep 1
	REDIS_URL=redis://127.0.0.1:$(REDIS_PORT) go test ./coax/... -count=1 -v -timeout 10m; \
		status=$$?; docker rm -f $(REDIS_NAME) >/dev/null; exit $$status

# Tens of thousands of sockets: both ends live in this process, so it needs a
# high file descriptor limit and a wide ephemeral port range.
.PHONY: load
load:
	CABLE_LOAD=1 CABLE_CONNS=$(CABLE_CONNS) go test ./coax/ -run TestManyIdleConnections \
		-count=1 -v -timeout 15m

.PHONY: conformance
conformance: autobahn jsclient redis

.PHONY: everything
everything: fmt vet race fuzz conformance load bench

.PHONY: example
example:
	go run ./cmd/example

.PHONY: doc
doc:
	go doc -all ./ws
	go doc -all ./coax
