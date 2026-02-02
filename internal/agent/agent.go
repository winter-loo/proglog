package agent

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/hashicorp/raft"
	"github.com/soheilhy/cmux"
	"github.com/winter-loo/proglog/internal/auth"
	"github.com/winter-loo/proglog/internal/discovery"
	"github.com/winter-loo/proglog/internal/errs"
	"github.com/winter-loo/proglog/internal/log"
	"github.com/winter-loo/proglog/internal/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Agent will integrate service discovery, server and replicator.

type Agent struct {
	Config

	mux        cmux.CMux
	log        *log.DistributedLog
	server     *grpc.Server
	membership *discovery.Membership

	shutdowns chan struct{}
	shutdown  bool
}

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
	BootStrap       bool
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", errs.Wrap(err)
	}
	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

func New(config Config) (_ *Agent, err error) {
	a := &Agent{
		Config:    config,
		shutdowns: make(chan struct{}),
	}

	setup := []func() error{
		a.setupLogger,
		a.setupMux,
		a.setupLog,
		a.setupServer,
		a.setupMemberShip,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}

	go a.serve()
	return a, nil
}

func (self *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	// set the newly created logger instance as the global logger for the entire
	// process (accessible via zap.L())
	zap.ReplaceGlobals(logger)
	return nil
}

func (self *Agent) setupMux() error {
	rpcAddr := fmt.Sprintf(":%d", self.Config.RPCPort)
	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return errs.Wrap(err)
	}
	self.mux = cmux.New(ln)
	return nil
}

// share RPCPort with distributed log's Raft networking
func (self *Agent) setupLog() error {
	var err error

	// get back the raft connection
	raftLn := self.mux.Match(func(reader io.Reader) bool {
		b := make([]byte, 1)
		if _, err := reader.Read(b); err != nil {
			return false
		}
		return bytes.Equal(b, []byte{byte(log.RaftRPC)})
	})
	networking := log.NewStreamLayer(
		raftLn,
		self.Config.ServerTLSConfig,
		self.Config.PeerTLSConfig,
	)

	config := log.Config{
		Raft: log.ConfigRaft{
			Config: raft.Config{
				LocalID:            raft.ServerID(self.Config.NodeName),
				HeartbeatTimeout:   50 * time.Millisecond,
				ElectionTimeout:    50 * time.Millisecond,
				LeaderLeaseTimeout: 50 * time.Millisecond,
				CommitTimeout:      5 * time.Millisecond,
			},
			StreamLayer: networking,
			Bootstrap:   self.Config.BootStrap,
		},
	}

	self.log, err = log.NewDistributedLog(
		self.Config.DataDir,
		config,
	)
	if err != nil {
		return err
	}

	// TODO: explain
	if self.Config.BootStrap {
		err = self.log.WaitForLeader(3 * time.Second)
	}

	return err
}

func (self *Agent) setupServer() error {
	authorizer, err := auth.New(
		self.Config.ACLModelFile,
		self.Config.ACLPolicyFile,
	)
	if err != nil {
		return err
	}

	serverConfig := &server.Config{
		CommitLog:  self.log,
		Authorizer: authorizer,
	}
	serverCreds := credentials.NewTLS(self.ServerTLSConfig)
	self.server, err = server.NewGRPCServer(serverConfig, grpc.Creds(serverCreds))
	if err != nil {
		return err
	}
	grpcLn := self.mux.Match(cmux.Any())
	go func() {
		if err := self.server.Serve(grpcLn); err != nil {
			_ = self.Shutdown()
		}
	}()
	return err
}

func (self *Agent) setupMemberShip() error {
	rpcAddr, err := self.RPCAddr()
	if err != nil {
		return err
	}

	dConfig := discovery.Config{
		NodeName: self.Config.NodeName,
		BindAddr: self.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: self.Config.StartJoinAddrs,
	}

	self.membership, err = discovery.New(self.log, dConfig)
	return err
}

func (self *Agent) Shutdown() error {
	if self.shutdown {
		return nil
	}

	self.shutdown = true

	shutdown := []func() error{
		self.membership.Leave,
		func() error {
			self.server.GracefulStop()
			return nil
		},
		self.log.Close,
	}

	for _, fn := range shutdown {
		fn()
	}
	return nil
}

func (self *Agent) serve() error {
	if err := self.mux.Serve(); err != nil {
		_ = self.Shutdown()
		return err
	}
	return nil
}
