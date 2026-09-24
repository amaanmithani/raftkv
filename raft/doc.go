// Package raft implements the Raft consensus algorithm as a pure,
// deterministic state machine: no I/O, no clocks, no goroutines. Drivers feed
// it ticks and messages and drain Ready batches. See docs/SPEC.md.
package raft
