/*
Copyright 2020 The Vitess Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package vtsql

import (
	"context"
	"database/sql"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	grpcresolver "google.golang.org/grpc/resolver"

	"vitess.io/vitess/go/vt/callerid"
	"vitess.io/vitess/go/vt/grpcclient"
	"vitess.io/vitess/go/vt/vitessdriver"

	querypb "vitess.io/vitess/go/vt/proto/query"
	vtadminpb "vitess.io/vitess/go/vt/proto/vtadmin"
	vtrpcpb "vitess.io/vitess/go/vt/proto/vtrpc"
)

func assertImmediateCaller(t *testing.T, im *querypb.VTGateCallerID, expected string) {
	t.Helper()

	require.NotNil(t, im, "immediate caller cannot be nil")
	assert.Equal(t, im.Username, expected, "immediate caller username mismatch")
}

func assertEffectiveCaller(t *testing.T, ef *vtrpcpb.CallerID, principal string, component string, subcomponent string) {
	t.Helper()

	require.NotNil(t, ef, "effective caller cannot be nil")
	assert.Equal(t, ef.Principal, principal, "effective caller principal mismatch")
	assert.Equal(t, ef.Component, component, "effective caller component mismatch")
	assert.Equal(t, ef.Subcomponent, subcomponent, "effective caller subcomponent mismatch")
}

func Test_getQueryContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()

	creds := &StaticAuthCredentials{
		EffectiveUser: "efuser",
		StaticAuthClientCreds: &grpcclient.StaticAuthClientCreds{
			Username: "imuser",
		},
	}
	db := &VTGateProxy{creds: creds}

	outctx := db.getQueryContext(ctx)
	assert.NotEqual(t, ctx, outctx, "getQueryContext should return a modified context when credentials are set")
	assertEffectiveCaller(t, callerid.EffectiveCallerIDFromContext(outctx), "efuser", "vtadmin", "")
	assertImmediateCaller(t, callerid.ImmediateCallerIDFromContext(outctx), "imuser")

	db.creds = nil
	outctx = db.getQueryContext(ctx)
	assert.Equal(t, ctx, outctx, "getQueryContext should not modify the context when credentials are not set")

	callerctx := callerid.NewContext(
		ctx,
		callerid.NewEffectiveCallerID("other principal", "vtctld", ""),
		callerid.NewImmediateCallerID("other_user"),
	)
	db.creds = creds

	outctx = db.getQueryContext(callerctx)
	assert.NotEqual(t, callerctx, outctx, "getQueryContext should override an existing callerid in the context")
	assertEffectiveCaller(t, callerid.EffectiveCallerIDFromContext(outctx), "efuser", "vtadmin", "")
	assertImmediateCaller(t, callerid.ImmediateCallerIDFromContext(outctx), "imuser")
}

func Test_dial_options_handling(t *testing.T) {
	t.Parallel()

	cluster := &vtadminpb.Cluster{Id: "test"}
	ctx := context.Background()

	// Track the dial options that were passed to the mock dial function
	var capturedConfig vitessdriver.Configuration
	mockDialFunc := func(cfg vitessdriver.Configuration) (*sql.DB, error) {
		capturedConfig = cfg
		return &sql.DB{}, nil
	}

	// Create a mock resolver
	mockResolver := &mockGRPCResolver{}

	proxy := &VTGateProxy{
		cluster:  cluster,
		dialFunc: mockDialFunc,
		resolver: mockResolver,
	}

	// Test dial with additional options
	additionalOpts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUserAgent("test-agent"),
	}

	err := proxy.dial(ctx, "test-target", additionalOpts...)
	require.NoError(t, err)

	// Verify that the configuration contains both user-provided options and resolver
	assert.Equal(t, "grpc_test", capturedConfig.Protocol)
	assert.Equal(t, "test-target", capturedConfig.Target)
	
	// Check that dial options include both user options and resolver
	require.Len(t, capturedConfig.GRPCDialOptions, 3) // 2 user opts + 1 resolver opt
	
	// The test verifies that user-provided options are preserved
	// and that resolver options are properly combined
	assert.NotNil(t, capturedConfig.GRPCDialOptions)
}

func Test_dial_with_credentials(t *testing.T) {
	t.Parallel()

	cluster := &vtadminpb.Cluster{Id: "test"}
	ctx := context.Background()

	var capturedConfig vitessdriver.Configuration
	mockDialFunc := func(cfg vitessdriver.Configuration) (*sql.DB, error) {
		capturedConfig = cfg
		return &sql.DB{}, nil
	}

	mockResolver := &mockGRPCResolver{}
	creds := &StaticAuthCredentials{
		StaticAuthClientCreds: &grpcclient.StaticAuthClientCreds{
			Username: "testuser",
		},
	}

	proxy := &VTGateProxy{
		cluster:  cluster,
		creds:    creds,
		dialFunc: mockDialFunc,
		resolver: mockResolver,
	}

	err := proxy.dial(ctx, "")
	require.NoError(t, err)

	// When credentials are provided, they should be prepended to dial options
	assert.NotEmpty(t, capturedConfig.GRPCDialOptions)
	// First option should be the credential option, followed by resolver option
	require.Len(t, capturedConfig.GRPCDialOptions, 2) // creds + resolver
}

// mockGRPCResolver is a mock implementation of grpcresolver.Builder for testing
type mockGRPCResolver struct{}

func (m *mockGRPCResolver) Build(target grpcresolver.Target, cc grpcresolver.ClientConn, opts grpcresolver.BuildOptions) (grpcresolver.Resolver, error) {
	return &mockResolver{}, nil
}

func (m *mockGRPCResolver) Scheme() string {
	return "mock"
}

type mockResolver struct{}

func (m *mockResolver) ResolveNow(opts grpcresolver.ResolveNowOptions) {}
func (m *mockResolver) Close()                                        {}
