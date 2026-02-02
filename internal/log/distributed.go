package log

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	api "github.com/winter-loo/proglog/api/v1"
	"github.com/winter-loo/proglog/internal/discovery"
	"github.com/winter-loo/proglog/internal/errs"
	"google.golang.org/protobuf/proto"
)

var _ discovery.Handler = (*DistributedLog)(nil)

type DistributedLog struct {
	config Config
	log    *Log
	raft   *raft.Raft
}

func NewDistributedLog(dataDir string, config Config) (*DistributedLog, error) {
	dlog := &DistributedLog{
		config: config,
	}

	if err := dlog.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := dlog.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return dlog, nil
}

func (self *DistributedLog) setupLog(dataDir string) error {
	logDir := filepath.Join(dataDir, "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	var err error
	self.log, err = NewLog(dataDir, self.config)
	if err != nil {
		return err
	}
	return nil
}

func (self *DistributedLog) setupRaft(dataDir string) error {
	fsm := &fsm{log: self.log}
	logDir := filepath.Join(dataDir, "raft", "log")
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return err
	}

	logConfig := self.config
	// TODO: explain this
	logConfig.Segment.InitialOffset = 1
	logStore, err := newLogStore(logDir, logConfig)
	if err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(
		filepath.Join(dataDir, "raft", "stable"),
	)
	if err != nil {
		return err
	}

	retain := 1

	snapshotStore, err := raft.NewFileSnapshotStore(
		filepath.Join(dataDir, "raft"),
		retain,
		os.Stderr,
	)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		self.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = self.config.Raft.LocalID

	if self.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = self.config.Raft.HeartbeatTimeout
	}
	if self.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = self.config.Raft.ElectionTimeout
	}
	if self.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = self.config.Raft.LeaderLeaseTimeout
	}
	if self.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = self.config.Raft.CommitTimeout
	}

	self.raft, err = raft.NewRaft(config, fsm, logStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(logStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	if self.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = self.raft.BootstrapCluster(config).Error()
	}

	return err
}

func (self *DistributedLog) Append(record *api.Record) (uint64, error) {
	res, err := self.apply(
		/*AppendRequetType,*/
		&api.ProduceRequest{
			Record: record,
		},
	)
	if err != nil {
		return 0, err
	}

	return res.(*api.ProduceResponse).Lsn, nil
}

func (self *DistributedLog) apply( /*reqType RequestType,*/ req proto.Message) (any, error) {
	cmd, err := proto.Marshal(req)
	if err != nil {
		return 0, errs.Wrap(err)
	}
	future := self.raft.Apply(cmd, self.config.Raft.CommitTimeout)
	err = future.Error()
	if err != nil {
		return 0, errs.Wrap(err)
	}
	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, errs.Wrap(err)
	}

	return res, nil
}

func (l *DistributedLog) Read(offset uint64) (*api.Record, error) {
	return l.log.Read(offset)
}

var _ raft.FSM = (*fsm)(nil)

type fsm struct {
	// The FSM must access the data it manages. Here, it's the Log.
	log *Log
}

type RequestType uint8

const (
	AppendRequestType RequestType = 0
)

// Raft invokes this method after committing a log entry
func (self *fsm) Apply(record *raft.Log) any {
	// After committing a log entry, we could write the log record into our local log store
	buf := record.Data

	return self.applyAppend(buf)
}

func (self *fsm) applyAppend(cmd []byte) any {
	var req api.ProduceRequest
	err := proto.Unmarshal(cmd, &req)
	if err != nil {
		return errs.Wrap(err)
	}

	lsn, err := self.log.Append(req.Record)
	if err != nil {
		return errs.Wrap(err)
	}

	return &api.ProduceResponse{Lsn: lsn}
}

// Raft periodically calls this method to snapshot its state.
func (self *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// will read all log data
	reader := self.log.Reader()
	return &snapshot{reader: reader}, nil
}

var _ raft.FSMSnapshot = (*snapshot)(nil)

type snapshot struct {
	reader io.Reader
}

// Raft calls Persist() on the FSMSnapshot we created to write its state to some
// sink that, depending on the snapshot store you configured Raft with, could be
// inmemory, a file, an S3 bucket—something to store the bytes in.
func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	if _, err := io.Copy(sink, s.reader); err != nil {
		_ = sink.Cancel()
		return errs.Wrap(err)
	}
	return errs.Wrap(sink.Close())
}

// Release implements raft.FSMSnapshot.
func (s *snapshot) Release() {

}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state before restoring the snapshot.
func (self *fsm) Restore(snapshot io.ReadCloser) error {
	// read the length field
	b := make([]byte, lenWidth)
	var buf bytes.Buffer

	for i := 0; ; i++ {
		_, err := io.ReadFull(snapshot, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return errs.Wrap(err)
		}

		size := int64(endian.Uint64(b))
		if _, err := io.CopyN(&buf, snapshot, size); err != nil {
			return errs.Wrap(err)
		}
		record := &api.Record{}
		if err := proto.Unmarshal(buf.Bytes(), record); err != nil {
			return errs.Wrap(err)
		}

		if i == 0 {
			self.log.Config.Segment.InitialOffset = record.Offset
			if err := self.log.Reset(); err != nil {
				return err
			}
		}
		if _, err := self.log.Append(record); err != nil {
			return err
		}
		buf.Reset()
	}
	return nil
}

// LogStore is used to provide an interface for storing
// and retrieving logs in a durable fashion.
var _ raft.LogStore = (*logStore)(nil)

type logStore struct {
	*Log
}

func newLogStore(dir string, c Config) (*logStore, error) {
	log, err := NewLog(dir, c)
	if err != nil {
		return nil, err
	}
	return &logStore{log}, nil
}

// DeleteRange implements raft.LogStore.
func (l *logStore) DeleteRange(min uint64, max uint64) error {
	return l.Truncate(max)
}

// FirstIndex implements raft.LogStore.
func (l *logStore) FirstIndex() (uint64, error) {
	return l.LowestOffset()
}

// GetLog implements raft.LogStore.
func (l *logStore) GetLog(index uint64, out *raft.Log) error {
	in, err := l.Read(index)
	if err != nil {
		return err
	}
	out.Data = in.Value
	out.Index = in.Offset
	out.Type = raft.LogType(in.Type)
	out.Term = in.Term
	return nil
}

// LastIndex implements raft.LogStore.
func (l *logStore) LastIndex() (uint64, error) {
	return l.HighestOffset()
}

// StoreLog implements raft.LogStore.
func (l *logStore) StoreLog(log *raft.Log) error {
	return l.StoreLogs([]*raft.Log{log})
}

// StoreLogs implements raft.LogStore.
func (l *logStore) StoreLogs(records []*raft.Log) error {
	for _, record := range records {
		if _, err := l.Append(&api.Record{
			Value:  record.Data,
			Offset: record.Index,
			Term:   record.Term,
			Type:   uint32(record.Type),
		}); err != nil {
			return err
		}
	}
	return nil
}

var _ raft.StreamLayer = (*StreamLayer)(nil)

type StreamLayer struct {
	ln net.Listener

	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

func NewStreamLayer(
	ln net.Listener,
	serverTLSConfig *tls.Config,
	peerTLSConfig *tls.Config,

) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

const RaftRPC = 1

// Accept implements raft.StreamLayer.
func (s *StreamLayer) Accept() (net.Conn, error) {
	cc, err := s.ln.Accept()
	if err != nil {
		return nil, errs.Wrap(err)
	}

	b := make([]byte, 1)
	_, err = cc.Read(b)
	if err != nil {
		return nil, errs.Wrap(err)
	}

	if !bytes.Equal([]byte{byte(RaftRPC)}, b) {
		return nil, fmt.Errorf("not a raft rpc")
	}
	if s.serverTLSConfig != nil {
		return tls.Server(cc, s.serverTLSConfig), nil
	}

	return cc, nil
}

// Addr implements raft.StreamLayer.
func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
}

// Close implements raft.StreamLayer.
func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	dialer := net.Dialer{Timeout: timeout}
	cc, err := dialer.Dial("tcp", string(address))
	if err != nil {
		return nil, errs.Wrap(err)
	}

	_, err = cc.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, errs.Wrap(err)
	}

	if s.peerTLSConfig != nil {
		cc = tls.Client(cc, s.peerTLSConfig)
	}

	return cc, nil
}

// When a server joins, update raft server list and make it be a voter
func (self *DistributedLog) OnJoin(_localName, peerName, peerRpcAddr string) error {
	configure := self.raft.GetConfiguration()
	if err := configure.Error(); err != nil {
		return errs.Wrap(err)
	}

	serverID := raft.ServerID(peerName)
	serverAddr := raft.ServerAddress(peerRpcAddr)

	// skip the same server or remove old conflicting server
	for _, srv := range configure.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				// server had already joined
				return nil
			}
			// conflict: remove the existing server
			removeFuture := self.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return errs.Wrap(err)
			}
		}
	}

	addFuture := self.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return errs.Wrap(err)
	}

	return nil
}

func (self *DistributedLog) OnLeave(name string) error {
	removeFuture := self.raft.RemoveServer(raft.ServerID(name), 0, 0)
	return errs.Wrap(removeFuture.Error())
}

func (self *DistributedLog) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	tickc := time.Tick(time.Second)
	for {
		select {
		case <-timeoutc:
			return errs.Wrap(fmt.Errorf("timed out"))
		case <-tickc:
			if l := self.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}

func (self *DistributedLog) Close() error {
	shutdownFuture := self.raft.Shutdown()
	if err := shutdownFuture.Error(); err != nil {
		return errs.Wrap(err)
	}
	return self.log.Close()
}
