package discovery

import (
	"net"

	"github.com/hashicorp/raft"
	"github.com/hashicorp/serf/serf"
	"go.uber.org/zap"
)

type Membership struct {
	Config
	handler Handler

	serf   *serf.Serf
	events chan serf.Event
	logger *zap.Logger
}

type Config struct {
	NodeName       string
	BindAddr       string
	Tags           map[string]string
	StartJoinAddrs []string
}

type Handler interface {
	OnJoin(localName, peerName, peerRpcAddr string) error
	OnLeave(name string) error
}

func New(handler Handler, config Config) (*Membership, error) {
	c := &Membership{
		Config:  config,
		handler: handler,
		logger:  zap.L().Named("membership"),
	}

	if err := c.setupSerf(); err != nil {
		return nil, err
	}

	return c, nil
}

// func	(self *Membership) Write(p []byte) (n int, err error) {
// 	return 0, nil
// }

func (self *Membership) setupSerf() error {
	addr, err := net.ResolveTCPAddr("tcp", self.BindAddr)
	if err != nil {
		return err
	}

	// obtain a default configuration
	config := serf.DefaultConfig()
	config.Init()

	config.MemberlistConfig.BindAddr = addr.IP.String()
	config.MemberlistConfig.BindPort = addr.Port

	self.events = make(chan serf.Event)
	config.EventCh = self.events
	config.Tags = self.Tags
	config.NodeName = self.NodeName
	// config.LogOutput = self

	// create the agent instance
	self.serf, err = serf.Create(config)
	if err != nil {
		return err
	}

	go self.eventHandler()

	if self.StartJoinAddrs != nil {
		_, err = self.serf.Join(self.StartJoinAddrs, true)
		if err != nil {
			return err
		}
	}

	return nil
}

func (self *Membership) eventHandler() {
	for e := range self.events {
		switch e.EventType() {
		case serf.EventMemberJoin:
			for _, member := range e.(serf.MemberEvent).Members {
				if self.isLocal(member) {
					continue
				}
				self.HandleJoin(member)
			}
		case serf.EventMemberLeave, serf.EventMemberFailed:
			{
				for _, member := range e.(serf.MemberEvent).Members {
					if self.isLocal(member) {
						continue
					}
					self.HandleLeave(member)
				}
			}
		}
	}
}

func (self *Membership) HandleLeave(member serf.Member) {
	if err := self.handler.OnLeave(member.Name); err != nil {
		self.logError(err, "failed to join", member)
	}
}

func (self *Membership) logError(err error, msg string, member serf.Member) {
	log := self.logger.Error
	if err == raft.ErrNotLeader {
		log = self.logger.Debug
	}
	log(msg,
		zap.Error(err),
		zap.String("name", member.Name),
		zap.String("rpc_addr", member.Tags["rpc_addr"]))
}

func (self *Membership) isLocal(member serf.Member) bool {
	return self.serf.LocalMember().Name == member.Name
}

func (self *Membership) HandleJoin(member serf.Member) {
	self.logger.Debug("member join",
		zap.String("node name", member.Name),
		zap.String("node addr", member.Addr.String()),
		zap.String("node rpc addr", member.Tags["rpc_addr"]),
		zap.Uint16("node port", member.Port),
	)
	if err := self.handler.OnJoin(self.serf.LocalMember().Name, member.Name, member.Tags["rpc_addr"]); err != nil {
		self.logError(err, "failed to join", member)
	}
}

func (self *Membership) Members() []serf.Member {
	return self.serf.Members()
}

func (self *Membership) Leave() error {
	return self.serf.Leave()
}
