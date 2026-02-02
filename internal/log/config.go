package log

import "github.com/hashicorp/raft"

type ConfigSegment struct {
	MaxStoreBytes uint64
	MaxIndexBytes uint64
	InitialOffset uint64
}

type ConfigRaft struct {
	raft.Config
	StreamLayer *StreamLayer
	Bootstrap   bool
}

type Config struct {
	Segment ConfigSegment
	Raft    ConfigRaft
}
