GO ?= go
CLI ?= python3 cli.py

.PHONY: help check quality ci test race bench vet fmt build check-routes check-filesize complexity architecture directory-fanout root-files root-business-code make-help coverage lint security-scan

help:
	@echo "make targets: check quality ci test race bench vet fmt build check-routes check-filesize complexity architecture directory-fanout root-files root-business-code make-help coverage lint security-scan"

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
