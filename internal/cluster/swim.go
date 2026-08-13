package cluster

import (
	"context"
	"time"

	counter "github.com/VeryFach/distributed-counter/api/proto"
	"github.com/VeryFach/distributed-counter/pkg/grpcutil"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// SWIMConfig configures the SWIM failure detector.
type SWIMConfig struct {
	NodeID     string
	Membership *Membership
	Logger     *zap.Logger

	// APIKey enables API key auth on probe connections (empty = disabled).
	APIKey string
	// Compression enables gRPC gzip on probe connections.
	Compression bool

	// ProtocolPeriod is how often a random member is probed.
	ProtocolPeriod time.Duration
	// ProbeTimeout bounds each individual PING/PING_REQ round.
	ProbeTimeout time.Duration
	// SuspectToDeadThreshold is the number of failed probes before a
	// Suspect member is escalated to Dead.
	SuspectToDeadThreshold int
}

// SWIMDetector implements the SWIM failure detection protocol: direct
// PING, indirect PING_REQ, and ACK. It actively probes random members and
// transitions them through Alive -> Suspect -> Dead.
type SWIMDetector struct {
	cfg    SWIMConfig
	ctx    context.Context
	cancel context.CancelFunc
}

func NewSWIMDetector(cfg SWIMConfig) *SWIMDetector {
	if cfg.ProtocolPeriod <= 0 {
		cfg.ProtocolPeriod = time.Second
	}
	if cfg.ProbeTimeout <= 0 {
		cfg.ProbeTimeout = 2 * time.Second
	}
	if cfg.SuspectToDeadThreshold <= 0 {
		cfg.SuspectToDeadThreshold = 3
	}
	if cfg.Logger == nil {
		cfg.Logger = zap.NewNop()
	}

	ctx, cancel := context.WithCancel(context.Background())
	return &SWIMDetector{cfg: cfg, ctx: ctx, cancel: cancel}
}

// Start runs the failure detector until Stop is called.
func (d *SWIMDetector) Start() {
	d.cfg.Logger.Info("Starting SWIM failure detector",
		zap.String("node_id", d.cfg.NodeID),
		zap.Duration("protocol_period", d.cfg.ProtocolPeriod),
		zap.Duration("probe_timeout", d.cfg.ProbeTimeout),
	)

	ticker := time.NewTicker(d.cfg.ProtocolPeriod)
	defer ticker.Stop()

	for {
		select {
		case <-d.ctx.Done():
			return
		case <-ticker.C:
			d.round()
		}
	}
}

// Stop terminates the detector loop.
func (d *SWIMDetector) Stop() {
	d.cancel()
}

func (d *SWIMDetector) round() {
	target := d.cfg.Membership.GetRandomProbeTarget()
	if target == nil {
		return
	}

	// Direct probe (PING / ACK).
	if d.probe(target) {
		d.cfg.Membership.UpdateHeartbeat(target.ID)
		return
	}

	// Direct probe failed; ask another member to probe on our behalf.
	if d.indirectProbe(target) {
		d.cfg.Membership.UpdateHeartbeat(target.ID)
		return
	}

	member, exists := d.cfg.Membership.GetMember(target.ID)
	if !exists {
		return
	}

	switch member.Status {
	case StatusAlive:
		d.cfg.Membership.MarkSuspect(target.ID)
		d.logStatusChange(member, StatusSuspect)
	case StatusSuspect:
		if member.SuspectCount+1 >= d.cfg.SuspectToDeadThreshold {
			d.cfg.Membership.MarkDead(target.ID)
			d.logStatusChange(member, StatusDead)
		} else {
			d.cfg.Membership.MarkSuspect(target.ID)
		}
	}
}

// probe sends a direct SWIM PING to the member.
func (d *SWIMDetector) probe(member *Member) bool {
	ctx, cancel := context.WithTimeout(d.ctx, d.cfg.ProbeTimeout)
	defer cancel()

	opts := append(grpcutil.DialOptions(d.cfg.APIKey, d.cfg.Compression), grpc.WithBlock())
	conn, err := grpc.DialContext(
		ctx,
		member.Address,
		opts...,
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := counter.NewCounterServiceClient(conn)
	resp, err := client.SwimPing(ctx, &counter.SwimPingRequest{
		FromNodeId:   d.cfg.NodeID,
		TargetNodeId: member.ID,
	})
	if err != nil {
		return false
	}
	return resp.Alive
}

// indirectProbe asks another random member (the proxy) to ping the target
// on our behalf (PING_REQ).
func (d *SWIMDetector) indirectProbe(target *Member) bool {
	proxy := d.cfg.Membership.GetRandomPeer()
	if proxy == nil || proxy.ID == target.ID {
		return false
	}

	ctx, cancel := context.WithTimeout(d.ctx, d.cfg.ProbeTimeout)
	defer cancel()

	opts := append(grpcutil.DialOptions(d.cfg.APIKey, d.cfg.Compression), grpc.WithBlock())
	conn, err := grpc.DialContext(
		ctx,
		proxy.Address,
		opts...,
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	client := counter.NewCounterServiceClient(conn)
	resp, err := client.SwimPingReq(ctx, &counter.SwimPingReqRequest{
		FromNodeId:   d.cfg.NodeID,
		TargetNodeId: target.ID,
	})
	if err != nil {
		return false
	}
	return resp.Alive
}

func (d *SWIMDetector) logStatusChange(member *Member, newStatus Status) {
	if d.cfg.Logger == nil {
		return
	}
	d.cfg.Logger.Warn("SWIM status change",
		zap.String("node_id", d.cfg.NodeID),
		zap.String("member", member.ID),
		zap.String("address", member.Address),
		zap.String("status", newStatus.String()),
	)
}