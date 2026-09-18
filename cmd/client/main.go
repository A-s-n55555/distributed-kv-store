package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	operation := flag.String(
		"op",
		"get",
		"Operation: put, get, delete, or resolve",
	)
	address := flag.String(
		"address",
		"localhost:50051",
		"gRPC server address",
	)
	key := flag.Int64("key", 0, "Key to use")
	value := flag.String("value", "", "Value for put operation")
	causalContext := flag.String(
		"context",
		"",
		"JSON causal context from Get, required for resolve",
	)

	resolveDeleted := flag.Bool(
		"deleted",
		false,
		"Resolve to a deletion tombstone",
	)
	flag.Parse()

	connection, err := grpc.NewClient(
		*address,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		log.Fatalf("failed to connect: %v", err)
	}
	defer connection.Close()

	client := kvpb.NewKeyValueStoreClient(connection)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	switch *operation {
	case "put":
		_, err := client.Put(ctx, &kvpb.PutRequest{
			Key:   *key,
			Value: *value,
		})
		if err != nil {
			log.Fatalf("Put failed: %v", err)
		}

		fmt.Println("Value stored successfully")

	case "get":
		response, err := client.Get(ctx, &kvpb.GetRequest{
			Key: *key,
		})
		if err != nil {
			log.Fatalf("Get failed: %v", err)
		}

		// Check conflicts before Found: tombstones can be siblings too.
		if response.GetConflict() {
			fmt.Println("Conflict: unresolved sibling versions")
		} else if !response.GetFound() {
			fmt.Println("Key not found")
		} else {
			fmt.Printf("Value: %s\n", response.GetValue())
		}

		versions := response.GetVersions()

		for i, record := range versions {
			clockJSON, err := json.Marshal(record.GetClock())
			if err != nil {
				log.Fatalf("Failed to encode version clock: %v", err)
			}

			if record.GetDeleted() {
				fmt.Printf(
					"Version %d: TOMBSTONE clock=%s\n",
					i+1,
					clockJSON,
				)
			} else {
				fmt.Printf(
					"Version %d: value=%q clock=%s\n",
					i+1,
					record.GetValue(),
					clockJSON,
				)
			}
		}

		if len(versions) > 0 {
			contextJSON, err := json.Marshal(response.GetContext())
			if err != nil {
				log.Fatalf("Failed to encode causal context: %v", err)
			}

			fmt.Printf("Context: %s\n", contextJSON)
		}

	case "delete":
		_, err := client.Delete(ctx, &kvpb.DeleteRequest{
			Key: *key,
		})
		if err != nil {
			log.Fatalf("Delete failed: %v", err)
		}

		fmt.Println("Key deleted successfully")

	case "resolve":
		if *causalContext == "" {
			log.Fatal("Resolve requires -context from a previous Get")
		}

		var clock map[string]uint64

		if err := json.Unmarshal(
			[]byte(*causalContext),
			&clock,
		); err != nil {
			log.Fatalf("Invalid context JSON: %v", err)
		}

		if len(clock) == 0 {
			log.Fatal("Resolution context must not be empty")
		}

		for nodeID, counter := range clock {
			if nodeID == "" || counter == 0 {
				log.Fatal("Context requires nonempty node IDs and positive counters")
			}
		}

		if *resolveDeleted && *value != "" {
			log.Fatal("Deletion resolution requires an empty -value")
		}

		_, err := client.Resolve(ctx, &kvpb.ResolveRequest{
			Key:     *key,
			Value:   *value,
			Context: clock,
			Deleted: *resolveDeleted,
		})
		if err != nil {
			log.Fatalf("Resolve failed: %v", err)
		}

		fmt.Println("Resolution write quorum reached; Get again to verify")

	default:
		log.Fatalf("unknown operation: %s", *operation)
	}
}
