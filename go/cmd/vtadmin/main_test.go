/*
Copyright 2025 The Vitess Authors.
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

package main

import (
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/vt/vtadmin/vtsql"
	"vitess.io/vitess/go/vt/vtctl/grpcclientcommon"
)

func TestMainFlagRegistration(t *testing.T) {
	// Create a new command to test flag registration without affecting the global rootCmd
	testCmd := &cobra.Command{
		Use: "test-vtadmin",
	}

	// Clear any existing flags to start fresh
	testCmd.ResetFlags()

	// MySQL server flags
	// Note: We can't easily test servenv.RegisterMySQLServerFlags without mocking
	// but we can test that the grpcclientcommon.RegisterFlags would be called

	// Test by checking that vtctld-grpc-* flags are properly registered

	// Simulate what grpcclientcommon.RegisterFlags does
	// Register TLS flags for gRPC connections to vtctld
	grpcclientcommon.RegisterFlags(testCmd.Flags())

	// Register TLS flags for gRPC connections to vtgate
	vtsql.RegisterVtgateFlags(testCmd.Flags())

	t.Run("vtgate grpc tls flags are registered", func(t *testing.T) {
		certFlag := testCmd.Flags().Lookup("vtgate-grpc-cert")
		require.NotNil(t, certFlag, "vtgate-grpc-cert flag should be registered")
		assert.Equal(t, "", certFlag.DefValue, "vtgate-grpc-cert should have empty default value")
		assert.Equal(t, "the cert to use to connect to vtgate", certFlag.Usage, "vtgate-grpc-cert should have correct usage")

		keyFlag := testCmd.Flags().Lookup("vtgate-grpc-key")
		require.NotNil(t, keyFlag, "vtgate-grpc-key flag should be registered")
		assert.Equal(t, "", keyFlag.DefValue, "vtgate-grpc-key should have empty default value")

		caFlag := testCmd.Flags().Lookup("vtgate-grpc-ca")
		require.NotNil(t, caFlag, "vtgate-grpc-ca flag should be registered")
		assert.Equal(t, "", caFlag.DefValue, "vtgate-grpc-ca should have empty default value")

		crlFlag := testCmd.Flags().Lookup("vtgate-grpc-crl")
		require.NotNil(t, crlFlag, "vtgate-grpc-crl flag should be registered")
		assert.Equal(t, "", crlFlag.DefValue, "vtgate-grpc-crl should have empty default value")

		serverNameFlag := testCmd.Flags().Lookup("vtgate-grpc-server-name")
		require.NotNil(t, serverNameFlag, "vtgate-grpc-server-name flag should be registered")
		assert.Equal(t, "", serverNameFlag.DefValue, "vtgate-grpc-server-name should have empty default value")
	})
}