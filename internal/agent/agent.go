package agent

import (
	"crypto/tls"
	"fmt"
	"net"

	api "github.com/winter-loo/proglog/api/v1"
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

	log        *log.Log
	server     *grpc.Server
	membership *discovery.Membership
	replicator *log.Replicator

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
		a.setupLog,
		a.setupServer,
		a.setupMemberShip,
	}

	for _, fn := range setup {
		if err := fn(); err != nil {
			return nil, err
		}
	}
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

func (self *Agent) setupLog() error {
	var err error
	self.log, err = log.NewLog(
		self.Config.DataDir,
		log.Config{},
	)
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
	rpcAddr, err := self.RPCAddr()
	if err != nil {
		return err
	}

	ln, err := net.Listen("tcp", rpcAddr)
	if err != nil {
		return err
	}

	go func() {
		if err := self.server.Serve(ln); err != nil {
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

	clientCreds := credentials.NewTLS(self.Config.PeerTLSConfig)
	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
	cc, err := grpc.NewClient(rpcAddr, clientOptions...)
	if err != nil {
		return errs.Wrap(err)
	}

	logClient := api.NewLogClient(cc)

	self.replicator = &log.Replicator{
		LocalServer: logClient,
	}

	dConfig := discovery.Config{
		NodeName: self.Config.NodeName,
		BindAddr: self.Config.BindAddr,
		Tags: map[string]string{
			"rpc_addr": rpcAddr,
		},
		StartJoinAddrs: self.Config.StartJoinAddrs,
	}

	self.membership, err = discovery.New(self.replicator, dConfig)
	if err != nil {
		return err
	}

	return nil
}

func (self *Agent) Shutdown() error {
	if self.shutdown {
		return nil
	}

	self.shutdown = true

	shutdown := []func() error{
		self.membership.Leave,
		self.replicator.Close,
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
