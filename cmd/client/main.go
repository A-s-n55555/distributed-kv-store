package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	kvpb "github.com/A-s-n55555/distributed-kv-store/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	operation := flag.String("op", "get", "Operation: put, get, or delete")
	address := flag.String(
		"address",
		"localhost:50051",
		"gRPC server address",
	)
	key := flag.Int64("key", 0, "Key to use")
	value := flag.String("value", "", "Value for put operation")
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

		if !response.GetFound() {
			fmt.Println("Key not found")
			return
		}

		fmt.Printf("Value: %s\n", response.GetValue())

	case "delete":
		_, err := client.Delete(ctx, &kvpb.DeleteRequest{
			Key: *key,
		})
		if err != nil {
			log.Fatalf("Delete failed: %v", err)
		}

		fmt.Println("Key deleted successfully")

	default:
		log.Fatalf("unknown operation: %s", *operation)
	}
}
