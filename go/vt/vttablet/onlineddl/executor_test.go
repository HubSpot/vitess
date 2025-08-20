/*
Copyright 2021 The Vitess Authors.

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

/*
Functionality of this Executor is tested in go/test/endtoend/onlineddl/...
*/

package onlineddl

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/mysql/fakesqldb"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/dbconfigs"
	"vitess.io/vitess/go/vt/vtenv"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/connpool"
	"vitess.io/vitess/go/vt/vttablet/tabletserver/tabletenv"
)

func TestShouldCutOverAccordingToBackoff(t *testing.T) {
	tcases := []struct {
		name string

		shouldForceCutOverIndicator bool
		forceCutOverAfter           time.Duration
		sinceReadyToComplete        time.Duration
		sinceLastCutoverAttempt     time.Duration
		cutoverAttempts             int64

		expectShouldCutOver      bool
		expectShouldForceCutOver bool
	}{
		{
			name:                "no reason why not, normal cutover",
			expectShouldCutOver: true,
		},
		{
			name:                "backoff",
			cutoverAttempts:     1,
			expectShouldCutOver: false,
		},
		{
			name:                "more backoff",
			cutoverAttempts:     3,
			expectShouldCutOver: false,
		},
		{
			name:                    "more backoff, since last cutover",
			cutoverAttempts:         3,
			sinceLastCutoverAttempt: time.Second,
			expectShouldCutOver:     false,
		},
		{
			name:                    "no backoff, long since last cutover",
			cutoverAttempts:         3,
			sinceLastCutoverAttempt: time.Hour,
			expectShouldCutOver:     true,
		},
		{
			name:                    "many attempts, long since last cutover",
			cutoverAttempts:         3000,
			sinceLastCutoverAttempt: time.Hour,
			expectShouldCutOver:     true,
		},
		{
			name:                        "force cutover",
			shouldForceCutOverIndicator: true,
			expectShouldCutOver:         true,
			expectShouldForceCutOver:    true,
		},
		{
			name:                        "force cutover overrides backoff",
			cutoverAttempts:             3,
			shouldForceCutOverIndicator: true,
			expectShouldCutOver:         true,
			expectShouldForceCutOver:    true,
		},
		{
			name:                     "backoff; cutover-after not in effect yet",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "backoff; cutover-after still not in effect yet",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     time.Millisecond,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "zero since ready",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     0,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "zero since read, zero cut-over-after",
			cutoverAttempts:          3,
			forceCutOverAfter:        0,
			sinceReadyToComplete:     0,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "microsecond",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Microsecond,
			sinceReadyToComplete:     time.Millisecond,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
		{
			name:                     "2 milliseconds, not ready",
			cutoverAttempts:          3,
			forceCutOverAfter:        2 * time.Millisecond,
			sinceReadyToComplete:     time.Millisecond,
			expectShouldCutOver:      false,
			expectShouldForceCutOver: false,
		},
		{
			name:                     "microsecond, ready irrespective of sinceReadyToComplete",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Millisecond,
			sinceReadyToComplete:     time.Microsecond,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
		{
			name:                     "cutover-after overrides backoff",
			cutoverAttempts:          3,
			forceCutOverAfter:        time.Second,
			sinceReadyToComplete:     time.Second * 2,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
		{
			name:                     "cutover-after overrides backoff, realistic value",
			cutoverAttempts:          300,
			sinceLastCutoverAttempt:  time.Minute,
			forceCutOverAfter:        time.Hour,
			sinceReadyToComplete:     time.Hour * 2,
			expectShouldCutOver:      true,
			expectShouldForceCutOver: true,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.name, func(t *testing.T) {
			shouldCutOver, shouldForceCutOver := shouldCutOverAccordingToBackoff(
				tcase.shouldForceCutOverIndicator,
				tcase.forceCutOverAfter,
				tcase.sinceReadyToComplete,
				tcase.sinceLastCutoverAttempt,
				tcase.cutoverAttempts,
			)
			assert.Equal(t, tcase.expectShouldCutOver, shouldCutOver)
			assert.Equal(t, tcase.expectShouldForceCutOver, shouldForceCutOver)
		})
	}
}

func TestSafeMigrationCutOverThreshold(t *testing.T) {
	require.NotZero(t, defaultCutOverThreshold)
	require.GreaterOrEqual(t, defaultCutOverThreshold, minCutOverThreshold)
	require.LessOrEqual(t, defaultCutOverThreshold, maxCutOverThreshold)

	tcases := []struct {
		threshold time.Duration
		expect    time.Duration
		isErr     bool
	}{
		{
			threshold: 0,
			expect:    defaultCutOverThreshold,
		},
		{
			threshold: 2 * time.Second,
			expect:    defaultCutOverThreshold,
			isErr:     true,
		},
		{
			threshold: 75 * time.Second,
			expect:    defaultCutOverThreshold,
			isErr:     true,
		},
		{
			threshold: defaultCutOverThreshold,
			expect:    defaultCutOverThreshold,
		},
		{
			threshold: 5 * time.Second,
			expect:    5 * time.Second,
		},
		{
			threshold: 15 * time.Second,
			expect:    15 * time.Second,
		},
		{
			threshold: 25 * time.Second,
			expect:    25 * time.Second,
		},
	}
	for _, tcase := range tcases {
		t.Run(tcase.threshold.String(), func(t *testing.T) {
			threshold, err := safeMigrationCutOverThreshold(tcase.threshold)
			if tcase.isErr {
				assert.Error(t, err)
				require.Equal(t, tcase.expect, defaultCutOverThreshold)
				// And keep testing, because we then also expect the threshold to be the default
			} else {
				assert.NoError(t, err)
			}
			assert.Equal(t, tcase.expect, threshold)
		})
	}
}

func TestAcquireTableLocksTimeout(t *testing.T) {
	// Create fake SQL DB that will simulate lock contention by blocking
	db := fakesqldb.New(t)
	defer db.Close()
	
	// Use a pattern to match LOCK TABLES queries
	lockQueryPattern := "LOCK TABLES.*WRITE.*WRITE"
	db.AddQueryPattern(lockQueryPattern, &sqltypes.Result{})
	
	// Set up blocking behavior for the specific query
	exactLockQuery := "LOCK TABLES `sentry_table` WRITE, `test_table` WRITE"
	db.AddQuery(exactLockQuery, &sqltypes.Result{})
	db.SetBeforeFunc(exactLockQuery, func() {
		// Block for longer than our timeout to simulate lock contention
		time.Sleep(12 * time.Second)
	})
	
	// Create proper tabletenv setup
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbconfigs.NewTestDBConfigs(*db.ConnParams(), *db.ConnParams(), db.ConnParams().DbName)
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TestExecutor")
	
	// Create connection pool and pooled connection
	pool := connpool.NewPool(env, "TestPool", tabletenv.ConnPoolConfig{
		Size:        1,
		IdleTimeout: 10 * time.Second,
	})
	pool.Open(cfg.DB.AppWithDB(), cfg.DB.DbaWithDB(), cfg.DB.AppDebugWithDB())
	defer pool.Close()
	
	pooledConn, err := pool.Get(context.Background(), nil)
	require.NoError(t, err)
	defer pooledConn.Recycle()
	
	// Add queries that the diagnostic functions might try to execute (but may fail due to connection pool exhaustion)
	db.AddQuery("SHOW FULL PROCESSLIST", &sqltypes.Result{})
	db.AddQuery("SELECT * FROM performance_schema.data_locks", &sqltypes.Result{})
	
	// Create a mock executor with minimal setup to avoid panics in diagnostic functions
	e := &Executor{
		env:  env,
		pool: pool,
	}
	ctx := context.Background()
	
	// Track if reenableWritesOnce was called
	reenableWritesCalled := false
	reenableWritesOnce := func() {
		reenableWritesCalled = true
	}
	
	start := time.Now()
	err = e.acquireTableLocks(ctx, pooledConn, "sentry_table", "test_table", "test-uuid", reenableWritesOnce)
	elapsed := time.Since(start)
	
	// Verify that the function returned an error due to timeout
	assert.Error(t, err)
	// The error can be either context deadline exceeded or MySQL's execution timeout
	assert.True(t, 
		strings.Contains(err.Error(), "context deadline exceeded") || 
		strings.Contains(err.Error(), "maximum statement execution time exceeded"),
		"Error should indicate a timeout: %v", err)
	
	// Verify that the timeout occurred within expected bounds 
	// Note: Should complete reasonably quickly due to diagnostic timeout (10s lock + ~4s diagnostic timeouts)
	assert.Greater(t, elapsed, 9*time.Second, "Lock acquisition should timeout after at least 9 seconds")
	assert.Less(t, elapsed, 17*time.Second, "Should complete within 17 seconds including diagnostic timeout attempts")
	
	// Verify that reenableWritesOnce was called when lock acquisition failed
	assert.True(t, reenableWritesCalled, "reenableWritesOnce should be called when lock acquisition fails")
}

func TestAcquireTableLocksWithDiagnostics(t *testing.T) {
	// Create fake SQL DB that will fail lock acquisition but allow diagnostics
	db := fakesqldb.New(t)
	defer db.Close()
	
	// Set up lock failure
	exactLockQuery := "LOCK TABLES `sentry_table` WRITE, `test_table` WRITE"
	db.AddQuery(exactLockQuery, &sqltypes.Result{})
	db.SetBeforeFunc(exactLockQuery, func() {
		// Simulate lock contention
		time.Sleep(12 * time.Second)
	})
	
	// Track diagnostic query execution
	processListCalled := false
	dataLocksCalled := false
	
	processListResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("Id|User|Host|db|Command|Time|State|Info", "int32|varchar|varchar|varchar|varchar|int32|varchar|varchar"),
		"1|root|localhost|test|Query|0|running|SELECT 1",
	)
	db.AddQuery("SHOW FULL PROCESSLIST", processListResult)
	db.SetBeforeFunc("SHOW FULL PROCESSLIST", func() {
		processListCalled = true
	})
	
	dataLocksResult := sqltypes.MakeTestResult(
		sqltypes.MakeTestFields("ENGINE|ENGINE_LOCK_ID|ENGINE_TRANSACTION_ID", "varchar|varchar|varchar"),
		"InnoDB|lock1|trx1",
	)
	db.AddQuery("SELECT * FROM performance_schema.data_locks", dataLocksResult)
	db.SetBeforeFunc("SELECT * FROM performance_schema.data_locks", func() {
		dataLocksCalled = true
	})
	
	// Create tabletenv setup with sufficient connections for diagnostics
	cfg := tabletenv.NewDefaultConfig()
	cfg.DB = dbconfigs.NewTestDBConfigs(*db.ConnParams(), *db.ConnParams(), db.ConnParams().DbName)
	env := tabletenv.NewEnv(vtenv.NewTestEnv(), cfg, "TestDiagnostics")
	
	// Create connection pool with multiple connections (>=3 as requested)
	pool := connpool.NewPool(env, "TestDiagnosticsPool", tabletenv.ConnPoolConfig{
		Size:        3, // Exactly 3 connections: 1 for lock, 2 for diagnostics
		IdleTimeout: 10 * time.Second,
	})
	pool.Open(cfg.DB.AppWithDB(), cfg.DB.DbaWithDB(), cfg.DB.AppDebugWithDB())
	defer pool.Close()
	
	pooledConn, err := pool.Get(context.Background(), nil)
	require.NoError(t, err)
	defer pooledConn.Recycle()
	
	// Create executor
	e := &Executor{
		env:  env,
		pool: pool,
	}
	ctx := context.Background()
	
	reenableWritesCalled := false
	reenableWritesOnce := func() {
		reenableWritesCalled = true
	}
	
	// Test lock acquisition with diagnostics
	start := time.Now()
	err = e.acquireTableLocks(ctx, pooledConn, "sentry_table", "test_table", "test-uuid", reenableWritesOnce)
	elapsed := time.Since(start)
	
	// Verify lock acquisition failed as expected
	assert.Error(t, err)
	assert.True(t, reenableWritesCalled, "reenableWritesOnce should be called")
	
	// Verify timeout behavior
	assert.Greater(t, elapsed, 9*time.Second, "Should timeout after at least 9 seconds")
	assert.Less(t, elapsed, 15*time.Second, "Should complete within 15 seconds including diagnostics")
	
	// Verify diagnostic functions were actually called when connections were available
	assert.True(t, processListCalled, "SHOW FULL PROCESSLIST should have been called for diagnostics")
	assert.True(t, dataLocksCalled, "performance_schema.data_locks should have been queried for diagnostics")
}
