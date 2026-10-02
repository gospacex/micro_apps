package nacos

import (
	"context"
	"errors"
	"os"
	"testing"
)

func TestTraceDataID(t *testing.T) {
	if TraceDataID != "hubx.trace" {
		t.Fatalf("TraceDataID = %q, want %q", TraceDataID, "hubx.trace")
	}
}

func TestLoadUnifiedPropagatesPrivateLoadError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	private, traceConfig, err := LoadUnified(ctx, "config.yaml", "hubx.gateway", "HUBX")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if private != nil || traceConfig != nil {
		t.Fatalf("handles = (%v, %v), want both nil", private, traceConfig)
	}
}

func TestLoadUnifiedFromNacos(t *testing.T) {
	if os.Getenv("HUBX_NACOS_INTEGRATION") != "1" {
		t.Skip("set HUBX_NACOS_INTEGRATION=1 to run against local Nacos")
	}

	private, traceConfig, err := LoadUnified(
		context.Background(),
		"../../apps/gateway/config.yaml",
		"hubx.gateway",
		"HUBX",
	)
	if err != nil {
		t.Fatalf("LoadUnified: %v", err)
	}
	defer func() { _ = private.Close() }()
	if traceConfig == nil {
		t.Fatal("trace config handle is nil")
	}
	defer func() { _ = traceConfig.Close() }()
	if !private.IsSet("gateway") {
		t.Fatal("private hubx.gateway dataId was not loaded")
	}
	if !traceConfig.IsSet("trace") {
		t.Fatal("unified hubx.trace dataId was not loaded")
	}
}
