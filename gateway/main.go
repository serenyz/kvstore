package gateway

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
)

const (
	defaultGatewayListen   = ":9120"
	defaultGatewayBackends = "1=127.0.0.1:9121"
	defaultShutdownTimeout = 5 * time.Second
)

// Main 解析 Gateway 参数并运行对外 gRPC 服务。
func Main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	flags := flag.NewFlagSet("gateway", flag.ContinueOnError)
	listenAddress := flags.String("listen", defaultGatewayListen, "gateway gRPC listen address")
	backendList := flags.String("backends", defaultGatewayBackends, "comma separated backend list in id=address form")
	shutdownTimeout := flags.Duration("shutdown-timeout", defaultShutdownTimeout, "graceful shutdown timeout")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *shutdownTimeout <= 0 {
		return errors.New("shutdown-timeout must be positive")
	}

	configs, err := parseBackendConfigs(*backendList)
	if err != nil {
		return err
	}
	pool, err := newBackendPool(configs)
	if err != nil {
		return fmt.Errorf("create backend pool: %w", err)
	}
	defer func() {
		if err := pool.close(); err != nil {
			log.Printf("close gateway backend pool: %v", err)
		}
	}()

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		return fmt.Errorf("listen for gateway RPC API on %q: %w", *listenAddress, err)
	}
	defer func() { _ = listener.Close() }()

	ctx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	return serve(ctx, listener, pool, *shutdownTimeout)
}

func serve(
	ctx context.Context,
	listener net.Listener,
	pool *backendPool,
	shutdownTimeout time.Duration,
) error {
	server, healthServer := newGatewayServer(pool)
	serverErrorC := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, grpc.ErrServerStopped) {
			err = nil
		}
		serverErrorC <- err
	}()

	select {
	case err := <-serverErrorC:
		healthServer.Shutdown()
		if err != nil {
			return fmt.Errorf("serve gateway RPC API: %w", err)
		}
		return nil
	case <-ctx.Done():
		healthServer.Shutdown()
		gracefulStop(server, shutdownTimeout)
		return nil
	}
}

func gracefulStop(server *grpc.Server, timeout time.Duration) {
	doneC := make(chan struct{})
	go func() {
		server.GracefulStop()
		close(doneC)
	}()

	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-doneC:
	case <-timer.C:
		server.Stop()
		<-doneC
	}
}

func parseBackendConfigs(value string) ([]backendConfig, error) {
	parts := strings.Split(value, ",")
	configs := make([]backendConfig, 0, len(parts))
	memberIDs := make(map[uint64]struct{}, len(parts))
	addresses := make(map[string]struct{}, len(parts))

	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		memberIDText, address, ok := strings.Cut(part, "=")
		memberIDText = strings.TrimSpace(memberIDText)
		address = strings.TrimSpace(address)
		if !ok || memberIDText == "" || address == "" {
			return nil, fmt.Errorf("invalid backend %q: expected id=address", part)
		}

		memberID, err := strconv.ParseUint(memberIDText, 10, 64)
		if err != nil || memberID == 0 {
			return nil, fmt.Errorf("invalid backend member ID %q", memberIDText)
		}
		if _, exists := memberIDs[memberID]; exists {
			return nil, fmt.Errorf("duplicate backend member ID %d", memberID)
		}
		if _, exists := addresses[address]; exists {
			return nil, fmt.Errorf("duplicate backend address %q", address)
		}

		memberIDs[memberID] = struct{}{}
		addresses[address] = struct{}{}
		configs = append(configs, backendConfig{id: memberID, address: address})
	}

	if len(configs) == 0 {
		return nil, errors.New("at least one backend is required")
	}
	return configs, nil
}
