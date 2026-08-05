# Engineering change gate

Every code or contract change in this repository must be followed by:

```sh
python3 cli.py quality
```

The gate checks:

- `gofmt` formatting;
- Go source file size and function-size/branch complexity thresholds from
  `engineering.yaml`;
- package dependency direction;
- repository root policy and security invariants;
- runtime routes against OpenAPI;
- `go vet`, unit tests, race tests, and production binary builds.

Do not report a change as complete while the quality gate is failing. If an
optional external scanner such as `gosec` is available, run
`python3 cli.py security-scan` as an additional release check.
