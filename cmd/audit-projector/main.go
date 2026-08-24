package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	cfg, err := resolveProjectorConfig(os.Args[1:], os.Getenv)
	if err != nil {
		log.Fatalf("config: %v", err)
	}

	logger := log.New(os.Stdout, "audit-projector ", log.LstdFlags|log.Lmicroseconds)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	if err := runProjector(ctx, cfg, logger, productionProjectorFactories()); err != nil && ctx.Err() == nil {
		logger.Fatalf("%v", err)
	}
	logger.Printf("shutting down")
}
