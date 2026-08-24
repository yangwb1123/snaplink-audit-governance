GO ?= go
CLI ?= python3 cli.py

.PHONY: help check quality ci test race bench vet fmt build proto migrate-pg check-routes check-filesize complexity architecture directory-fanout root-files root-business-code make-help coverage lint security-scan web-install web-dev web-test web-build web-check web-stack-up web-stack-down web-auth-e2e e2e-tenant-scope e2e-projector-permanent-dlq

help:
	@echo "make targets: check quality ci test race bench vet fmt build proto migrate-pg check-routes check-filesize complexity architecture directory-fanout root-files root-business-code make-help coverage lint security-scan web-install web-dev web-test web-build web-check web-stack-up web-stack-down web-auth-e2e e2e-tenant-scope e2e-projector-permanent-dlq"

check:
	$(CLI) check

quality:
	$(CLI) quality

ci:
	$(CLI) ci

test:
	$(CLI) test

race:
	$(CLI) race

bench:
	$(CLI) bench

vet:
	$(CLI) vet

fmt:
	$(CLI) fmt

build:
	$(CLI) build

proto:
	python3 scripts/proto-gen.py

migrate-pg:
	go run ./cmd/audit-pg-migrate -confirm MIGRATE

check-routes:
	$(CLI) check-routes

check-filesize:
	$(CLI) check-filesize

complexity:
	$(CLI) complexity

architecture:
	$(CLI) architecture

directory-fanout:
	$(CLI) directory-fanout

root-files:
	$(CLI) root-files

root-business-code:
	$(CLI) root-business-code

make-help:
	$(CLI) make-help

coverage:
	$(CLI) coverage

lint:
	$(CLI) lint

security-scan:
	$(CLI) security-scan

web-install:
	corepack pnpm install --dir web

web-dev:
	corepack pnpm --dir web dev

web-test:
	corepack pnpm --dir web test

web-build:
	corepack pnpm --dir web build

web-check:
	corepack pnpm --dir web check

web-stack-up:
	bash test/e2e/web-stack.sh

web-stack-down:
	docker compose -f deploy/docker-compose.verify.yml --profile web down

web-auth-e2e:
	bash test/e2e/web-auth.sh

e2e-tenant-scope:
	test/e2e/consumer-tenant-scope.sh

e2e-projector-permanent-dlq:
	test/e2e/projector-permanent-dlq.sh
