package log_test

import (
	"fmt"
	"net"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/stretchr/testify/require"
	"github.com/travisjeffery/go-dynaport"
	api "github.com/winter-loo/proglog/api/v1"
	"github.com/winter-loo/proglog/internal/config"
	"github.com/winter-loo/proglog/internal/log"
)

func TestDistributedLog(t *testing.T) {
	var dlogs []*log.DistributedLog
	nodeCount := 3
	ports := dynaport.Get(nodeCount)

	for i := range nodeCount {
		dataDir, err := os.MkdirTemp("", "distributed-log-test")
		require.NoError(t, err)
		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CAFile:        config.CAFile,
			CertFile:      config.ServerCertFile,
			KeyFile:       config.ServerKeyFile,
			ServerAddress: ln.Addr().String(),
			Server:        true,
		})
		require.NoError(t, err)

		peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CAFile:   config.CAFile,
			CertFile: config.RootClientCertFile,
			KeyFile:  config.RootClientKeyFile,
			Server:   false,
		})
		require.NoError(t, err)

		networking := log.NewStreamLayer(ln, serverTLSConfig, peerTLSConfig)

		config := log.Config{
			Raft: log.ConfigRaft{
				Config: raft.Config{
					LocalID:            raft.ServerID(fmt.Sprintf("%d", i)),
					HeartbeatTimeout:   50 * time.Millisecond,
					ElectionTimeout:    50 * time.Millisecond,
					LeaderLeaseTimeout: 50 * time.Millisecond,
					CommitTimeout:      5 * time.Millisecond,
				},
				StreamLayer: networking,
				Bootstrap:   false,
			},
		}
		if i == 0 {
			config.Raft.Bootstrap = true
		}

		dlog, err := log.NewDistributedLog(dataDir, config)
		require.NoError(t, err)

		if i != 0 {
			err = dlogs[0].OnJoin(
				fmt.Sprintf("%d", 0), fmt.Sprintf("%d", i), ln.Addr().String(),
			)
			require.NoError(t, err)
		} else {
			err = dlog.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		dlogs = append(dlogs, dlog)
	}

	records := []*api.Record{
		{Value: []byte("first")},
		{Value: []byte("second")},
	}

	for _, rec := range records {
		lsn, err := dlogs[0].Append(rec)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := range nodeCount {
				got, err := dlogs[j].Read(lsn)
				require.NoError(t, err)
				if !reflect.DeepEqual(got.Value, rec.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)
	}

	// checks that the leader stops replicating to a server that's left the cluster,
	// while continuing to replicate to the existing servers.

	err := dlogs[0].OnLeave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	lsn, err := dlogs[0].Append(&api.Record{Value: []byte("third")})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	record, err := dlogs[1].Read(lsn)
	require.IsType(t, api.ErrLsnOutOfRange{}, err)
	require.Nil(t, record)

	record, err = dlogs[2].Read(lsn)
	require.NoError(t, err)
	require.Equal(t, []byte("third"), record.Value)
	require.Equal(t, lsn, record.Offset)
}
