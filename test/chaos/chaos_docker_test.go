//go:build chaosdocker

// Package chaos (chaosdocker build tag) runs a chaos experiment against a
// running docker-compose cluster. The cluster must already be up (see
// scripts/chaos-test.ps1 / chaos-test.sh). It injects container-level chaos
// (pause/unpause and restart) while a load generator keeps incrementing, then
// verifies that the cluster converges to the exact total of successful
// increments once every container is healthy again.
package chaos

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	counter "github.com/VeryFach/distributed-counter/api/proto"
)

const (
	composeFile      = "deployments/docker-compose.yml"
	chaosServices    = "node-a,node-b,node-c"
	chaosPorts       = "50051,50052,50053"
	dockerChaosEvery = 500 * time.Millisecond
)

func repoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return ""
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return ""
		}
		dir = parent
	}
}

func TestChaosDocker(t *testing.T) {
	if !dockerAvailable() {
		t.Skip("docker not available")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	ports := strings.Split(chaosPorts, ",")
	services := strings.Split(chaosServices, ",")

	// Resolve each service to its container id so chaos can target it.
	containers := make([]string, len(services))
	for i, svc := range services {
		id, err := containerID(svc)
		if err != nil {
			t.Fatalf("resolve container for %s: %v", svc, err)
		}
		containers[i] = id
	}

	conns := make([]*grpc.ClientConn, len(ports))
	clients := make([]counter.CounterServiceClient, len(ports))
	for i, port := range ports {
		conn, err := grpc.DialContext(
			ctx,
			"localhost:"+port,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		)
		if err != nil {
			t.Fatalf("dial %s: %v", port, err)
		}
		conns[i] = conn
		clients[i] = counter.NewCounterServiceClient(conn)
	}
	t.Cleanup(func() {
		for _, conn := range conns {
			if conn != nil {
				_ = conn.Close()
			}
		}
	})

	// Deterministic baseline.
	for _, c := range clients {
		if _, err := c.Reset(ctx, &counter.ResetRequest{}); err != nil {
			t.Fatalf("reset: %v", err)
		}
	}
	waitDockerConverged(t, ctx, clients, 0, 20*time.Second)

	var (
		loadWG  sync.WaitGroup
		chaosWG sync.WaitGroup
		stop    atomic.Bool
		success atomic.Int64
		errCh   = make(chan error, 64)
	)

	// Load generator: increment a random node.
	loadWG.Add(1)
	go func() {
		defer loadWG.Done()
		for !stop.Load() {
			i := rand.Intn(len(clients))

			callCtx, callCancel := context.WithTimeout(ctx, time.Second)
			_, err := clients[i].Increment(callCtx, &counter.IncrementRequest{Delta: 1})
			callCancel()
			if err == nil {
				success.Add(1)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Chaos driver: pause/unpause or restart random containers.
	chaosWG.Add(1)
	go func() {
		defer chaosWG.Done()
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			i := rand.Intn(len(containers))
			switch rand.Intn(2) {
			case 0: // pause, then unpause after a moment
				t.Logf("chaos: pause %s", services[i])
				if out, err := dockerCmd("pause", containers[i]); err != nil {
					errCh <- fmt.Errorf("docker pause %s: %v (%s)", services[i], err, out)
				} else {
					time.Sleep(1500 * time.Millisecond)
					if out, err := dockerCmd("unpause", containers[i]); err != nil {
						errCh <- fmt.Errorf("docker unpause %s: %v (%s)", services[i], err, out)
					}
				}
			case 1: // full container restart
				t.Logf("chaos: restart %s", services[i])
				if out, err := dockerCmd("restart", containers[i]); err != nil {
					errCh <- fmt.Errorf("docker restart %s: %v (%s)", services[i], err, out)
				}
			}
			time.Sleep(dockerChaosEvery)
		}
	}()

	chaosWG.Wait()
	stop.Store(true)
	loadWG.Wait()

	// Ensure every container is back up and unpaused.
	for i, id := range containers {
		_ = exec.Command("docker", "unpause", id).Run()
		_ = exec.Command("docker", "start", id).Run()
		t.Logf("recovered %s (%s)", services[i], id)
	}

	expected := success.Load()
	t.Logf("issued %d successful increments", expected)

	close(errCh)
	for err := range errCh {
		t.Error(err)
	}

	// Because a paused/restarted node may apply an increment whose response is
	// then lost (at-least-once delivery), every node must converge to a common
	// value that is at least the number of client-observed successes.
	waitDockerConverged(t, ctx, clients, expected, 45*time.Second)
}

func waitDockerConverged(t *testing.T, ctx context.Context, clients []counter.CounterServiceClient, expectedMin int64, timeout time.Duration) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for {
		values := make([]int64, len(clients))
		ok := true
		for i, c := range clients {
			resp, err := c.GetValue(ctx, &counter.GetValueRequest{})
			if err != nil {
				ok = false
				break
			}
			values[i] = resp.CurrentValue
			if values[i] != values[0] {
				ok = false
			}
		}

		if ok && values[0] >= expectedMin {
			t.Logf("cluster converged to %d (min expected %d)", values[0], expectedMin)
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("cluster did not converge: min-expected=%d values=%v", expectedMin, values)
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func dockerAvailable() bool {
	out, err := exec.Command("docker", "version").CombinedOutput()
	return err == nil && strings.Contains(string(out), "Client")
}

func dockerCmd(args ...string) (string, error) {
	cmd := exec.Command("docker", args...)
	out, err := cmd.CombinedOutput()
	return strings.TrimSpace(string(out)), err
}

func containerID(service string) (string, error) {
	root := repoRoot()
	if root == "" {
		return "", fmt.Errorf("could not locate repository root (go.mod)")
	}
	out, err := dockerCmd("compose", "-f", filepath.Join(root, composeFile), "ps", "-q", service)
	if err != nil {
		return "", err
	}
	id := strings.TrimSpace(out)
	if id == "" {
		return "", fmt.Errorf("no container for service %s", service)
	}
	return id, nil
}
