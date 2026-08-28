// Command openapi-contract validates the checked-in OpenAPI document with the
// pinned kin-openapi version used by the repository quality gate.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/getkin/kin-openapi/openapi3"
)

const (
	contractPath     = "api/openapi/openapi.yaml"
	validatorVersion = "kin-openapi v0.148.0"
)

func main() {
	path := contractPath
	if len(os.Args) == 2 {
		path = os.Args[1]
	} else if len(os.Args) > 2 {
		fmt.Fprintln(os.Stderr, "usage: openapi-contract [path]")
		os.Exit(2)
	}

	loader := openapi3.NewLoader()
	loader.IsExternalRefsAllowed = false
	document, err := loader.LoadFromFile(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "openapi: load %s: %v\n", path, err)
		os.Exit(1)
	}
	if document.OpenAPI != "3.1.0" {
		fmt.Fprintf(os.Stderr, "openapi: %s declares %q, want 3.1.0\n", path, document.OpenAPI)
		os.Exit(1)
	}
	if err := document.Validate(context.Background()); err != nil {
		fmt.Fprintf(os.Stderr, "openapi: validate %s: %v\n", path, err)
		os.Exit(1)
	}
	fmt.Printf("openapi: valid %s (%s)\n", path, validatorVersion)
}
