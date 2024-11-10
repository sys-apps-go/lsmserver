package main

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"google.golang.org/grpc"
	pb "github.com/rkmngkr/lsmdb/lsm"
)

var (
	serverAddr  = flag.String("server_addr", "localhost:50051", "The server address in the format of host:port")
	numUsers    = flag.Int("num_users", 5, "Number of concurrent users")
	opsPerUser  = flag.Int("ops_per_user", 10, "Number of operations per user")
	keyRange    = flag.Int("key_range", 100, "Range of keys (0 to key_range-1)")
	valueRange  = flag.Int("value_range", 1000, "Range of values (0 to value_range-1)")
	sleepMillis = flag.Int("sleep_millis", 500, "Sleep time in milliseconds between operations")
)

func main() {
	flag.Parse()

	conn, err := grpc.Dial(*serverAddr, grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()

	client := pb.NewDatabaseClient(conn)

	var wg sync.WaitGroup
	wg.Add(*numUsers)

	for i := 0; i < *numUsers; i++ {
		go func(userID int) {
			defer wg.Done()

			rand.Seed(time.Now().UnixNano())

			for j := 0; j < *opsPerUser; j++ {
				key := strconv.Itoa(rand.Intn(*keyRange))
				value := strconv.Itoa(rand.Intn(*valueRange))

				op := rand.Intn(5) // Randomly select between 0, 1, 2, 3, 4
				switch op {
				case 0:
					setResponse, err := client.Set(context.Background(), &pb.SetRequest{Key: key, Value: value})
					if err != nil {
						log.Printf("Set failed: %v", err)
					} else {
						log.Printf("Set: %v", setResponse.Success)
					}
				case 1:
					getResponse, err := client.Get(context.Background(), &pb.GetRequest{Key: key})
					if err != nil {
						log.Printf("Get failed: %v", err)
					} else {
						log.Printf("Get: %v", getResponse.Value)
					}
				case 2:
					deleteResponse, err := client.Delete(context.Background(), &pb.DeleteRequest{Key: key})
					if err != nil {
						log.Printf("Delete failed: %v", err)
					} else {
						log.Printf("Delete: %v", deleteResponse.Success)
					}
				case 3:
					listPushResponse, err := client.ListPush(context.Background(), &pb.ListPushRequest{Key: key, Value: value})
					if err != nil {
						log.Printf("ListPush failed: %v", err)
					} else {
						log.Printf("ListPush: %v", listPushResponse.Success)
					}
				case 4:
					listPopResponse, err := client.ListPop(context.Background(), &pb.ListPopRequest{Key: key})
					if err != nil {
						log.Printf("ListPop failed: %v", err)
					} else {
						log.Printf("ListPop: %v", listPopResponse.Value)
					}
				}

				time.Sleep(time.Duration(*sleepMillis) * time.Millisecond)
			}
		}(i)
	}

	wg.Wait()
	log.Println("Test complete")
}
