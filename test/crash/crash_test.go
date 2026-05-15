// Package crash_test contains subprocess crash-recovery tests.
// Each test writes N records, sends SIGKILL at a random point,
// restarts the engine, and verifies all WAL-committed records are recoverable.
package crash_test
