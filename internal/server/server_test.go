package server

import (
	"context"
	"flag"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	api "github.com/winter-loo/proglog/api/v1"
	"github.com/winter-loo/proglog/internal/auth"
	"github.com/winter-loo/proglog/internal/config"
	"github.com/winter-loo/proglog/internal/log"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()
	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}
	os.Exit(m.Run())
}

func TestServer(t *testing.T) {
	for testCase, fn := range map[string]func(
		t *testing.T,
		rootClient api.LogClient,
		nobodyClient api.LogClient,
		config *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"produce/consume stream succeeds":                    testProduceConsumeStream,
		"consume past log boundary fails":                    testConsumePastBoundary,
		"unauthorized fails":                                 testUnauthorized,
	} {
		t.Run(testCase, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (api.LogClient, api.LogClient, *Config, func()) {
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

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.LogClient,
		[]grpc.DialOption,
	) {

		// https://blog.cloudflare.com/how-to-build-your-own-public-key-infrastructure
		clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			// mutual TLS authentication
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
		})
		require.NoError(t, err)
		clientCreds := credentials.NewTLS(clientTLSConfig)
		clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(clientCreds)}

		cc, err := grpc.NewClient(l.Addr().String(), clientOptions...)
		require.NoError(t, err)

		client := api.NewLogClient(cc)
		return cc, client, clientOptions
	}

	rootConn, rootClient, _ := newClient(config.RootClientCertFile, config.RootClientKeyFile)
	nobodyConn, nobodyClient, _ := newClient(config.NobodyClientCertFile, config.NobodyClientKeyFile)

	dir, err := os.MkdirTemp("", "server-test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	authorizer, err := auth.New(config.ACLModelFile, config.ACLPolicyFile)
	require.NoError(t, err)
	cfg := &Config{
		CommitLog:  clog,
		Authorizer: authorizer,
	}

	var meterProvider *sdkmetric.MeterProvider
	var tracerProvider *sdktrace.TracerProvider
	var metricsLogFile *os.File
	var tracesLogFile *os.File

	if *debug {
		// 1. Setup Metrics Log File
		metricsLogFile, err := os.CreateTemp("", "metrics-*.log")
		require.NoError(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		// 2. Setup Traces Log File
		tracesLogFile, err := os.CreateTemp("", "traces-*.log")
		require.NoError(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		// 3. Configure OTel Metrics Exporter (writes to metricsLogFile)
		metricExporter, err := stdoutmetric.New(
			stdoutmetric.WithWriter(metricsLogFile),
			stdoutmetric.WithPrettyPrint(), // Optional: makes JSON readable like OC
		)
		require.NoError(t, err)

		// 4. Configure Meter Provider with Periodic Reader (replaces ReportingInterval)
		meterProvider = sdkmetric.NewMeterProvider(
			sdkmetric.WithReader(sdkmetric.NewPeriodicReader(
				metricExporter,
				sdkmetric.WithInterval(1*time.Second),
			)),
		)
		otel.SetMeterProvider(meterProvider)

		// 5. Configure OTel Trace Exporter (writes to tracesLogFile)
		traceExporter, err := stdouttrace.New(
			stdouttrace.WithWriter(tracesLogFile),
			stdouttrace.WithPrettyPrint(),
		)
		require.NoError(t, err)

		// 6. Configure Tracer Provider
		tracerProvider = sdktrace.NewTracerProvider(
			sdktrace.WithBatcher(traceExporter),
		)
		otel.SetTracerProvider(tracerProvider)

		require.NoError(t, err)
	}

	if fn != nil {
		fn(cfg)
	}

	serverTlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:        config.CAFile,
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		ServerAddress: l.Addr().String(),
		// enable mutual TLS authentication
		Server: true,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTlsConfig)

	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	require.NoError(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		ctx := context.Background()

		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		clog.Remove()

		if meterProvider != nil {
			meterProvider.Shutdown(ctx)
		}
		if tracerProvider != nil {
			tracerProvider.Shutdown(ctx)
		}
		if metricsLogFile != nil {
			metricsLogFile.Close()
		}
		if tracesLogFile != nil {
			tracesLogFile.Close()
		}

		// Restore proxy environment variables
		for key, val := range savedEnv {
			os.Setenv(key, val)
		}
	}
}

func testProduceConsume(
	t *testing.T,
	client, _ api.LogClient,
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
	client, _ api.LogClient,
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
	client, _ api.LogClient,
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

func testUnauthorized(
	t *testing.T,
	_, client api.LogClient,
	config *Config,
) {
	ctx := context.Background()
	_, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Value: []byte("hello world"),
		},
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want code %d, got %d", codes.PermissionDenied, status.Code(err))
	}

	_, err = client.Consume(ctx, &api.ConsumeRequest{
		Lsn: 0,
	})
	if status.Code(err) != codes.PermissionDenied {
		t.Fatalf("want code %d, got %d", codes.PermissionDenied, status.Code(err))
	}
}
