GO ?= go
CLI ?= python3 cli.py

.PHONY: help check quality ci test race bench vet fmt build proto check-routes check-filesize complexity architecture directory-fanout root-files root-business-code make-help coverage lint security-scan e2e-tenant-scope e2e-projector-permanent-dlq

help:
	@echo "make targets: check quality ci test race bench vet fmt build proto check-routes check-filesize complexity architecture directory-fanout root-files root-business-code make-help coverage lint security-scan e2e-tenant-scope e2e-projector-permanent-dlq"

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

e2e-tenant-scope:
	test/e2e/consumer-tenant-scope.sh

e2e-projector-permanent-dlq:
	test/e2e/projector-permanent-dlq.sh
