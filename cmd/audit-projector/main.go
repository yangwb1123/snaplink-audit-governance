package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "inspect" {
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		if err := runInspectCLI(ctx, os.Args[2:], os.Stdout); err != nil {
			log.Fatalf("%v", err)
		}
		return
	}
	if len(os.Args) > 1 && os.Args[1] == "rebuild" {
		logger := log.New(os.Stdout, "audit-projection-rebuild ", log.LstdFlags|log.Lmicroseconds)
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
		defer stop()
		if err := runRebuildCLI(ctx, os.Args[2:], logger); err != nil {
			logger.Fatalf("%v", err)
		}
		return
	}
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
