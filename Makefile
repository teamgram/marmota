# Marmota base library - common targets for test, race, vulncheck and benchmark.
# Use: make test, make test-race, make govulncheck, make bench

.PHONY: test test-race govulncheck bench

test:
	go test ./...

test-race:
	go test -race ./...

govulncheck:
	go install golang.org/x/vuln/cmd/govulncheck@latest
	govulncheck ./...

bench:
	go test -bench=. -benchmem -count=5 ./pkg/...
