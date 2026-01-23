package log

import (
	"context"
	"io"
	"strings"
	"sync"

	api "github.com/winter-loo/proglog/api/v1"
	"github.com/winter-loo/proglog/internal/config"
	"github.com/winter-loo/proglog/internal/discovery"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

// make sure Replicator implementing discovery.Handler interface
var _ discovery.Handler = (*Replicator)(nil)

type Replicator struct {
	LocalServer api.LogClient
	logger      *zap.Logger
	close       chan struct{}
	closed      bool

	mu      sync.Mutex
	servers map[string]chan struct{}
	// wg tracks active replication goroutines to ensure they finish before Close returns.
	wg sync.WaitGroup
}

// Replicator is built on top of service discovery module(discovery.Membership).
//
// When this replicator joins a cluster, it starts pulling records from peers.
// Once pulled a record, it sends this record to current local server. Current
// local server stores the record.
func (self *Replicator) replicate(localName string, peerNodeName string, peerRpcAddr string, leave chan struct{}) {
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		CertFile: config.RootClientCertFile,
		KeyFile:  config.RootClientKeyFile,
	})
	if err != nil {
		self.logError(err, "failed to setup tls config", peerRpcAddr)
		return
	}
	clientCreds := credentials.NewTLS(clientTLSConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
	cc, err := grpc.NewClient(peerRpcAddr, opts...)
	if err != nil {
		self.logError(err, "failed to dial", peerRpcAddr)
		return
	}
	defer cc.Close()

	client := api.NewLogClient(cc)

	// The gRPC methods `ConsumeStream` and `Produce` require a `context.Context`
	// as their first argument to manage the request lifecycle (deadlines,
	// cancellation and metadata).
	ctx := context.Background()
	logStream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Lsn: 0})
	if err != nil {
		self.logError(err, "failed to consume stream", peerRpcAddr)
		return
	}

	recordChan := make(chan *api.Record)

	go func() {
		for {
			recv, err := logStream.Recv()
			if err != nil {
				if err == io.EOF ||
					status.Code(err) == codes.Canceled ||
					strings.Contains(err.Error(), "client connection is closing") {
					return
				}
				self.logError(err, "failed to recv", peerRpcAddr)
				return
			}
			self.logger.Debug("pulled log",
				zap.String("local node", localName),
				zap.String("from node", peerNodeName),
				zap.Uint64("lsn", recv.Record.Offset),
			)
			recordChan <- recv.Record
		}
	}()

	for {
		select {
		case <-self.close:
			return
		case <-leave:
			return
		case record := <-recordChan:
			_, err := self.LocalServer.Produce(ctx, &api.ProduceRequest{
				Record: record,
			})
			if err != nil {
				self.logError(err, "failed to produce", peerRpcAddr)
				return
			}
		}
	}
}

func (self *Replicator) OnJoin(localName, peerName, peerRpcAddr string) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.init()

	if self.closed {
		return nil
	}

	if _, ok := self.servers[peerName]; ok {
		// already replicating from this newly joining node so to skip
		return nil
	}

	self.servers[peerName] = make(chan struct{})

	// Increment WaitGroup before starting the replication goroutine.
	self.wg.Add(1)
	go func() {
		// Decrement WaitGroup when the replication goroutine exits.
		defer self.wg.Done()
		self.replicate(localName, peerName, peerRpcAddr, self.servers[peerName])
	}()

	return nil
}

func (self *Replicator) init() {
	if self.logger == nil {
		self.logger = zap.L().Named("replicator")
	}

	if self.servers == nil {
		self.servers = make(map[string]chan struct{})
	}

	if self.close == nil {
		self.close = make(chan struct{})
	}
}

func (self *Replicator) logError(err error, msg string, addr string) {
	self.logger.Error(
		msg,
		zap.Error(err),
		zap.String("addr", addr),
	)
}

func (self *Replicator) OnLeave(name string) error {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.init()

	if _, ok := self.servers[name]; !ok {
		return nil
	}

	// close the channel to signify leaving
	close(self.servers[name])
	delete(self.servers, name)
	return nil
}

func (self *Replicator) Close() error {
	self.mu.Lock()
	defer self.mu.Unlock()

	self.init()

	if self.closed {
		return nil
	}
	self.closed = true
	// Signal all replication goroutines to stop.
	close(self.close)
	// Wait for all replication goroutines to finish their work and exit cleanly.
	self.wg.Wait()
	return nil
}
