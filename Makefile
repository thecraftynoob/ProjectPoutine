# ProjectPoutine — root Makefile
#
# On Windows, run these from a shell where `go` and `buf` are on PATH, e.g.
# PowerShell after:
#   $env:Path = [System.Environment]::GetEnvironmentVariable("Path","Machine") + ";" + [System.Environment]::GetEnvironmentVariable("Path","User")
# Then run via `make <target>` (Git Bash / WSL / make for Windows) or copy
# the underlying command directly into PowerShell.

.PHONY: proto-gen build test tidy vet compose-up compose-down

proto-gen:
	cd proto && buf generate

build:
	go build ./...

test:
	go test ./...

vet:
	go vet ./...

tidy:
	go mod tidy

compose-up:
	docker compose up -d

compose-down:
	docker compose down
