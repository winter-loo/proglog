package server

import (
	"context"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	api "github.com/winter-loo/proglog/api/v1"
	"github.com/winter-loo/proglog/internal/config"
	"github.com/winter-loo/proglog/internal/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

func TestServer(t *testing.T) {
	for testCase, fn := range map[string]func(
		t *testing.T,
		client api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
	} {
		t.Run(testCase, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (api.LogClient, *Config, func()) {
	t.Helper()

	// Capture and unset proxy environment variables to avoid interfering with local gRPC connections
	proxyVars := []string{"http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"}
	savedEnv := make(map[string]string)
	for _, key := range proxyVars {
		if val, ok := os.LookupEnv(key); ok {
			savedEnv[key] = val
			os.Unsetenv(key)
		}
	}

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// https://blog.cloudflare.com/how-to-build-your-own-public-key-infrastructure
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile: config.CAFile,
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(clientTLSConfig)
	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}
	cc, err := grpc.NewClient(l.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg := &Config{
		CommitLog: clog,
	}

	if fn != nil {
		fn(cfg)
	}

	serverTlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:        config.CAFile,
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		ServerAddress: l.Addr().String(),
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTlsConfig)

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	client := api.NewLogClient(cc)
	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		clog.Remove()
		// Restore proxy environment variables
		for key, val := range savedEnv {
			os.Setenv(key, val)
		}
	}
}

func testProduceConsume(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	want := &api.Record{
		Value: []byte("hello world"),
	}
	pr, err := client.Produce(ctx, &api.ProduceRequest{
		Record: want,
	})
	require.NoError(t, err)

	cr, err := client.Consume(ctx, &api.ConsumeRequest{
		Lsn: pr.Lsn,
	})
	require.NoError(t, err)

	require.Equal(t, want.Value, cr.Record.Value)
	require.Equal(t, want.Offset, cr.Record.Offset)
}

func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()

	records := []*api.Record{{
		Value:  []byte("first message"),
		Offset: 0,
	}, {
		Value:  []byte("second message"),
		Offset: 1,
	}}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)
		for offset, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			require.NoError(t, err)
			res, err := stream.Recv()
			require.NoError(t, err)
			if res.Lsn != uint64(offset) {
				t.Fatalf(
					"got offset: %d, want: %d",
					res.Lsn,
					offset,
				)
			}
		}
	}

	{
		stream, err := client.ConsumeStream(
			ctx,
			&api.ConsumeRequest{Lsn: 0},
		)
		require.NoError(t, err)
		for i, record := range records {
			res, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, res.Record, &api.Record{
				Value:  record.Value,
				Offset: uint64(i),
			})

		}
	}
}

func testConsumePastBoundary(
	t *testing.T,
	client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	require.NoError(t, err)
	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Lsn: produce.Lsn + 1,
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrLsnOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want: %v", got, want)
	}
}
