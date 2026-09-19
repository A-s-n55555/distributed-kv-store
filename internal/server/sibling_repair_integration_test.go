package server

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/A-s-n55555/distributed-kv-store/internal/ring"
	"github.com/A-s-n55555/distributed-kv-store/internal/store"
	"github.com/A-s-n55555/distributed-kv-store/internal/version"
	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestQuorumReadRepairsSplitSiblings(t *testing.T) {
	for _, deleted := range []bool{false, true} {
		name := "two live siblings"
		if deleted {
			name = "live and tombstone siblings"
		}

		t.Run(name, func(t *testing.T) {
			firstServer := newRecordTestServer(t)
			secondServer := newRecordTestServer(t)

			servers := []*GRPCServer{firstServer, secondServer}
			nodeIDs := []string{"node-1", "node-2"}
			addresses := make([]string, 2)
			clusterRing := ring.New(16)

			// Configure everything before starting request handling.
			for i, s := range servers {
				s.nodeID = nodeIDs[i]
				s.ring = clusterRing
				s.replicationFactor = 2
				s.readQuorum = 2
				s.writeQuorum = 2
			}

			for i, s := range servers {
				listener, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}

				addresses[i] = listener.Addr().String()
				clusterRing.AddNode(ring.Node{
					ID:      nodeIDs[i],
					Address: addresses[i],
				})

				grpcServer := grpc.NewServer()
				kvpb.RegisterKeyValueStoreServer(grpcServer, s)

				done := make(chan struct{})
				go func() {
					defer close(done)
					_ = grpcServer.Serve(listener)
				}()

				t.Cleanup(func() {
					grpcServer.Stop()
					<-done
				})
			}

			const key int64 = 9500

			first := store.Record{
				Value: "first",
				Clock: version.Clock{"node-1": 1},
			}
			second := store.Record{
				Clock:   version.Clock{"node-2": 1},
				Deleted: deleted,
			}
			if !deleted {
				second.Value = "second"
			}

			// Intentionally split the siblings between replicas.
			if err := firstServer.store.ApplyRecord(key, first); err != nil {
				t.Fatal(err)
			}
			if err := secondServer.store.ApplyRecord(key, second); err != nil {
				t.Fatal(err)
			}

			connection, err := grpc.NewClient(
				addresses[0],
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer connection.Close()

			client := kvpb.NewKeyValueStoreClient(connection)
			ctx, cancel := context.WithTimeout(
				context.Background(),
				10*time.Second,
			)
			defer cancel()

			response, err := client.Get(ctx, &kvpb.GetRequest{Key: key})
			if err != nil {
				t.Fatalf("Get failed: %v", err)
			}

			if !response.GetConflict() ||
				!response.GetFound() ||
				len(response.GetVersions()) != 2 {
				t.Fatalf("expected two visible siblings: %+v", response)
			}

			if response.GetContext()["node-1"] != 1 ||
				response.GetContext()["node-2"] != 1 {
				t.Fatalf("incorrect context: %v", response.GetContext())
			}

			// Inspect each store directly. Another public Get could
			// trigger repair and hide a failure in the first request.
			for i, s := range servers {
				actual := s.store.GetRecords(key)
				if len(actual) != 2 {
					t.Fatalf(
						"%s has %d versions; want 2",
						nodeIDs[i],
						len(actual),
					)
				}

				for _, expected := range []store.Record{first, second} {
					found := false

					for _, record := range actual {
						if record.Value == expected.Value &&
							record.Deleted == expected.Deleted &&
							version.Compare(
								record.Clock,
								expected.Clock,
							) == version.Equal {
							found = true
							break
						}
					}

					if !found {
						t.Fatalf(
							"%s missing %+v; stored %+v",
							nodeIDs[i],
							expected,
							actual,
						)
					}
				}
			}
		})
	}
}
