package main

import (
	"context"
	"encoding/json"
	"log"
	"net"
	"os"
	"sync"

	pb "github.com/rkmngkr/lsmdb/lsm"
	"google.golang.org/grpc"
)

const (
	walFilePath  = ".wal.log"
	mvccFilePath = ".mvcc.log"
	sstFilePath  = ".sst.log"
	cacheSize    = 100
)

type walEntry struct {
	Operation string
	Key       string
	Value     string
	Version   int
}

type mvccEntry struct {
	Key     string
	Version int
}

type sstEntry struct {
	Key   string
	Value string
}

type versionedValue struct {
	Value   string
	Version int
}

type server struct {
	pb.UnimplementedDatabaseServer
	mu            sync.RWMutex
	store         map[string][]versionedValue
	listStore     map[string][]string
	latestVersion int
	cache         map[string]versionedValue
	cacheCount    int
}

func (s *server) Set(ctx context.Context, req *pb.SetRequest) (*pb.SetResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latestVersion++

	s.store[req.Key] = append(s.store[req.Key], versionedValue{Value: req.Value, Version: s.latestVersion})

	walEntry := walEntry{Operation: "SET", Key: req.Key, Value: req.Value, Version: s.latestVersion}
	if err := s.writeWAL(walEntry); err != nil {
		return &pb.SetResponse{Success: false}, err
	}

	mvccEntry := mvccEntry{Key: req.Key, Version: s.latestVersion}
	if err := s.writeMVCC(mvccEntry); err != nil {
		return &pb.SetResponse{Success: false}, err
	}

	sstEntry := sstEntry{Key: req.Key, Value: req.Value}
	if err := s.writeSST(sstEntry); err != nil {
		return &pb.SetResponse{Success: false}, err
	}

	s.addToCache(req.Key, versionedValue{Value: req.Value, Version: s.latestVersion})
	return &pb.SetResponse{Success: true}, nil
}

func (s *server) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if val, found := s.cache[req.Key]; found {
		return &pb.GetResponse{Value: val.Value}, nil
	}

	if vals, found := s.store[req.Key]; found && len(vals) > 0 {
		return &pb.GetResponse{Value: vals[len(vals)-1].Value}, nil
	}

	return &pb.GetResponse{Value: ""}, nil
}

func (s *server) Delete(ctx context.Context, req *pb.DeleteRequest) (*pb.DeleteResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latestVersion++

	s.store[req.Key] = append(s.store[req.Key], versionedValue{Value: "", Version: s.latestVersion})

	walEntry := walEntry{Operation: "DELETE", Key: req.Key, Value: "", Version: s.latestVersion}
	if err := s.writeWAL(walEntry); err != nil {
		return &pb.DeleteResponse{Success: false}, err
	}

	mvccEntry := mvccEntry{Key: req.Key, Version: s.latestVersion}
	if err := s.writeMVCC(mvccEntry); err != nil {
		return &pb.DeleteResponse{Success: false}, err
	}

	delete(s.cache, req.Key)
	return &pb.DeleteResponse{Success: true}, nil
}

func (s *server) ListPush(ctx context.Context, req *pb.ListPushRequest) (*pb.ListPushResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latestVersion++

	s.listStore[req.Key] = append(s.listStore[req.Key], req.Value)

	walEntry := walEntry{Operation: "LIST_PUSH", Key: req.Key, Value: req.Value, Version: s.latestVersion}
	if err := s.writeWAL(walEntry); err != nil {
		return &pb.ListPushResponse{Success: false}, err
	}

	mvccEntry := mvccEntry{Key: req.Key, Version: s.latestVersion}
	if err := s.writeMVCC(mvccEntry); err != nil {
		return &pb.ListPushResponse{Success: false}, err
	}

	return &pb.ListPushResponse{Success: true}, nil
}

func (s *server) ListPop(ctx context.Context, req *pb.ListPopRequest) (*pb.ListPopResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.latestVersion++

	values, exists := s.listStore[req.Key]
	if !exists || len(values) == 0 {
		return &pb.ListPopResponse{Value: "", Success: false}, nil
	}

	value := values[len(values)-1]
	s.listStore[req.Key] = values[:len(values)-1]

	walEntry := walEntry{Operation: "LIST_POP", Key: req.Key, Value: value, Version: s.latestVersion}
	if err := s.writeWAL(walEntry); err != nil {
		return &pb.ListPopResponse{Value: "", Success: false}, err
	}

	mvccEntry := mvccEntry{Key: req.Key, Version: s.latestVersion}
	if err := s.writeMVCC(mvccEntry); err != nil {
		return &pb.ListPopResponse{Value: "", Success: false}, err
	}

	return &pb.ListPopResponse{Value: value, Success: true}, nil
}

func (s *server) writeWAL(entry walEntry) error {
	file, err := os.OpenFile(walFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(entry)
}

func (s *server) writeMVCC(entry mvccEntry) error {
	file, err := os.OpenFile(mvccFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(entry)
}

func (s *server) writeSST(entry sstEntry) error {
	file, err := os.OpenFile(sstFilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	return encoder.Encode(entry)
}

func (s *server) loadWAL() error {
	file, err := os.Open(walFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var entry walEntry
		if err := decoder.Decode(&entry); err != nil {
			break
		}

		s.mu.Lock()
		switch entry.Operation {
		case "SET":
			s.store[entry.Key] = append(s.store[entry.Key], versionedValue{Value: entry.Value, Version: entry.Version})
		case "DELETE":
			s.store[entry.Key] = append(s.store[entry.Key], versionedValue{Value: "", Version: entry.Version})
		case "LIST_PUSH":
			s.listStore[entry.Key] = append(s.listStore[entry.Key], entry.Value)
		case "LIST_POP":
			values := s.listStore[entry.Key]
			if len(values) > 0 {
				s.listStore[entry.Key] = values[:len(values)-1]
			}
		}
		if entry.Version > s.latestVersion {
			s.latestVersion = entry.Version
		}
		s.mu.Unlock()
	}

	return nil
}

func (s *server) loadMVCC() error {
	file, err := os.Open(mvccFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var entry mvccEntry
		if err := decoder.Decode(&entry); err != nil {
			break
		}

		s.mu.Lock()
		s.store[entry.Key] = append(s.store[entry.Key], versionedValue{Version: entry.Version})
		if entry.Version > s.latestVersion {
			s.latestVersion = entry.Version
		}
		s.mu.Unlock()
	}

	return nil
}

func (s *server) loadSST() error {
	file, err := os.Open(sstFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var entry sstEntry
		if err := decoder.Decode(&entry); err != nil {
			break
		}

		s.mu.Lock()
		s.store[entry.Key] = append(s.store[entry.Key], versionedValue{Value: entry.Value})
		s.mu.Unlock()
	}

	return nil
}

func (s *server) addToCache(key string, value versionedValue) {
	if s.cacheCount >= cacheSize {
		for k := range s.cache {
			delete(s.cache, k)
			break
		}
		s.cacheCount--
	}

	s.cache[key] = value
	s.cacheCount++
}

func main() {
	s := &server{
		store:         make(map[string][]versionedValue),
		listStore:     make(map[string][]string),
		latestVersion: 0,
		cache:         make(map[string]versionedValue),
	}

	// Recovery flag
	if len(os.Args) > 1 && os.Args[1] == "-r" {
		if err := s.loadWAL(); err != nil {
			log.Fatalf("failed to load WAL: %v", err)
		}
		if err := s.loadMVCC(); err != nil {
			log.Fatalf("failed to load MVCC: %v", err)
		}
		if err := s.loadSST(); err != nil {
			log.Fatalf("failed to load SST: %v", err)
		}
	}

	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	grpcServer := grpc.NewServer()
	pb.RegisterDatabaseServer(grpcServer, s)

	log.Println("Server is running on port 50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}
