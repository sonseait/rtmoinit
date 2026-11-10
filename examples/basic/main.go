package main

import (
	"context"
	"errors"
	"log"
	"math"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gofiber/fiber/v3"
	"github.com/redis/go-redis/v9"
	"github.com/sonseait/rtmoinit"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var client *redis.Client
	if address := os.Getenv("REDIS_ADDR"); address != "" {
		client = redis.NewClient(&redis.Options{Addr: address, Username: os.Getenv("REDIS_USERNAME"), Password: os.Getenv("REDIS_PASSWORD"), ContextTimeoutEnabled: true, MaxRetries: -1, DialTimeout: 3 * time.Second})
		defer client.Close()
	}
	monitor, err := rtmoinit.New(ctx, rtmoinit.Config{
		Redis: client, Namespace: "rtmoinit-demo",
		Definition: rtmoinit.Definition{
			Series: []rtmoinit.Series{
				{ID: "requests", Title: "Requests", Kind: rtmoinit.Counter},
				{ID: "latency", Title: "Latency", Kind: rtmoinit.Gauge, Unit: "ms"},
			},
			Tables: []rtmoinit.Table{{ID: "workers", Columns: []rtmoinit.Column{{Key: "name", Label: "Worker"}, {Key: "state", Label: "State"}, {Key: "jobs", Label: "Jobs"}}}},
			Dashboards: []rtmoinit.Dashboard{{ID: "overview", Title: "Application overview", Panels: []rtmoinit.Panel{
				{ID: "requests", Title: "Total requests", Type: rtmoinit.Stat, Series: []string{"requests"}},
				{ID: "latency", Title: "Current latency", Type: rtmoinit.Stat, Series: []string{"latency"}},
				{ID: "trend", Title: "Latency over time", Type: rtmoinit.Line, Series: []string{"latency"}, Width: 12},
				{ID: "throughput", Title: "Cumulative requests", Type: rtmoinit.Bar, Series: []string{"requests"}},
				{ID: "workers", Title: "Worker status", Type: rtmoinit.TablePanel, Table: "workers"},
			}}},
		},
	})
	if err != nil {
		return err
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		if err := monitor.Close(closeCtx); err != nil {
			log.Printf("final checkpoint: %v", err)
		}
	}()
	app := fiber.New(fiber.Config{WriteTimeout: 10 * time.Second})
	// Attach application authentication middleware to this group in production.
	monitor.Mount(app.Group("/monitor"))
	sampleDone := make(chan struct{})
	go func() {
		defer close(sampleDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		jobs := 0
		for {
			select {
			case <-ctx.Done():
				return
			case now := <-ticker.C:
				jobs++
				if err := errors.Join(monitor.Add("requests", 1), monitor.Set("latency", 40+25*math.Sin(float64(now.UnixMilli())/3000)), monitor.SetRows("workers", []rtmoinit.Row{{"name": "demo-worker", "state": "running", "jobs": jobs}})); err != nil {
					log.Printf("update: %v", err)
					return
				}
			}
		}
	}()
	address := os.Getenv("LISTEN_ADDR")
	if address == "" {
		address = "127.0.0.1:3000"
	}
	serverErr := make(chan error, 1)
	go func() { serverErr <- app.Listen(address, fiber.ListenConfig{DisableStartupMessage: true}) }()
	log.Printf("dashboard: http://%s/monitor/", address)
	select {
	case err := <-serverErr:
		stop()
		<-sampleDone
		return err
	case <-ctx.Done():
		<-sampleDone
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
		defer cancel()
		return errors.Join(monitor.Close(shutdownCtx), app.ShutdownWithContext(shutdownCtx))
	}
}
