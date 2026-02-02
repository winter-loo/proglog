package agent_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github.com/winter-loo/proglog/api/v1"
	"github.com/winter-loo/proglog/internal/agent"
	"github.com/winter-loo/proglog/internal/config"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:        config.CAFile,
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:        config.CAFile,
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var agents []*agent.Agent
	for i := range 3 {
		ports := dynaport.Get(2)
		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", ports[0])
		rpcPort := ports[1]

		dataDir, err := os.MkdirTemp("", "agent-test-log")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			BindAddr:        bindAddr,
			StartJoinAddrs:  startJoinAddrs,
			RPCPort:         rpcPort,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
			BootStrap:       i == 0,
		})
		require.NoError(t, err)
		zap.L().Log(zap.DebugLevel, "agent setup", zap.Int("rpcPort", rpcPort), zap.String("service discovery addr", bindAddr))

		agents = append(agents, agent)
	}
	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)
			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()
	fmt.Println("wait all 3 agents setup...")
	time.Sleep(3 * time.Second)
	fmt.Println("all 3 agents setup...DONE")

	leaderClient := client(t, agents[0])

	value := []byte("foo")
	ctx := context.Background()
	produceResponse, err := leaderClient.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: value,
		},
	})
	require.NoError(t, err)

	consumed, err := leaderClient.Consume(ctx, &api.ConsumeRequest{Lsn: produceResponse.Lsn})
	require.NoError(t, err)
	require.Equal(t, consumed.Record.Value, value)

	fmt.Println("wait for eventual consistency...")
	time.Sleep(3 * time.Second)
	fmt.Println("eventual consistency...DONE")

	// for i := 1; i < 3; i++ {
	followerClient := client(t, agents[1])
	consumed, err = followerClient.Consume(ctx, &api.ConsumeRequest{Lsn: produceResponse.Lsn})
	require.NoError(t, err)
	require.Equal(t, consumed.Record.Value, value)
	// }

	consumed, err = leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{Lsn: produceResponse.Lsn + 1},
	)
	require.Nil(t, consumed)
	require.Error(t, err)
	got := status.Code(err)
	want := status.Code(api.ErrLsnOutOfRange{}.GRPCStatus().Err())
	require.Equal(t, got, want)
}

func client(t *testing.T, agent *agent.Agent) api.LogClient {
	rpcAddr, err := agent.RPCAddr()
	require.NoError(t, err)

	clientCreds := grpc.WithTransportCredentials(credentials.NewTLS(agent.PeerTLSConfig))
	cc, err := grpc.NewClient(rpcAddr, clientCreds)
	require.NoError(t, err)

	client := api.NewLogClient(cc)
	return client
}
