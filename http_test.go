package rtmoinit

import (
	"bufio"
	"context"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gofiber/fiber/v3"
)

func TestSSEReconnectLimitAndShutdown(t *testing.T) {
	cfg := testConfig()
	cfg.MaxClients = 1
	m := newTestMonitor(t, cfg)
	app := fiber.New(fiber.Config{WriteTimeout: 2 * time.Second})
	m.Mount(app.Group("/monitor"))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan error, 1)
	go func() { served <- app.Listener(listener, fiber.ListenConfig{DisableStartupMessage: true}) }()
	t.Cleanup(func() {
		_ = m.Close(context.Background())
		_ = app.ShutdownWithTimeout(3 * time.Second)
		if err := <-served; err != nil {
			t.Error(err)
		}
	})
	client := &http.Client{Timeout: 4 * time.Second}
	url := "http://" + listener.Addr().String() + "/monitor/events"
	response, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != 200 || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatal("invalid SSE response")
	}
	scanner := bufio.NewScanner(response.Body)
	if !scanner.Scan() || scanner.Text() != "event: snapshot" {
		t.Fatal("missing initial snapshot")
	}
	if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "data: ") {
		t.Fatal("missing snapshot data")
	}
	blocked, err := client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	blocked.Body.Close()
	if blocked.StatusCode != 429 {
		t.Fatalf("client limit ignored: %d", blocked.StatusCode)
	}
	response.Body.Close()
	deadline := time.Now().Add(3 * time.Second)
	for len(m.clients) > 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if len(m.clients) != 0 {
		t.Fatal("disconnected client slot leaked")
	}
	if err = m.Add("count", 9); err != nil {
		t.Fatal(err)
	}
	response, err = client.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatalf("reconnect failed: %d", response.StatusCode)
	}
	scanner = bufio.NewScanner(response.Body)
	for scanner.Scan() {
		if strings.HasPrefix(scanner.Text(), "data: ") {
			if !strings.Contains(scanner.Text(), `"value":9`) {
				t.Fatal("reconnect did not get current data")
			}
			break
		}
	}
	if err = scanner.Err(); err != nil {
		t.Fatal(err)
	}
	if err = m.Close(context.Background()); err != nil {
		t.Fatal(err)
	}
}
