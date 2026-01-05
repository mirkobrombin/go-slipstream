package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mirkobrombin/go-slipstream/pkg/engine"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"

	badger "github.com/dgraph-io/badger/v4"
	redis "github.com/redis/go-redis/v9"
	bolt "go.etcd.io/bbolt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	concurrency = flag.Int("c", 50, "Concurrency")
	requests    = flag.Int("n", 100000, "Total requests")
	dataSize    = flag.Int("d", 256, "Payload size in bytes")
	target      = flag.String("target", "all", "Target: slipstream, bolt, badger, redis, mongodb")
	redisAddr   = flag.String("redis-addr", "localhost:6379", "Redis Address")
	mongoAddr   = flag.String("mongo-addr", "mongodb://localhost:27017", "MongoDB URI")
	dataDir     = flag.String("data-dir", "/tmp/bench-slipstream", "Data directory")
)

func main() {
	flag.Parse()

	payload := make([]byte, *dataSize)
	for i := range payload {
		payload[i] = 'x'
	}

	targets := strings.Split(*target, ",")
	if *target == "all" {
		targets = []string{"slipstream", "bolt", "badger", "redis", "mongodb"}
	}

	fmt.Printf("| %-15s | %-12s | %-12s | %-12s | %-12s |\n", "System", "Write Ops/s", "Read Ops/s", "Avg Lat (ns)", "P99 Lat (ns)")
	fmt.Println("|:---|:---|:---|:---|:---|")

	for _, t := range targets {
		runBenchmark(strings.TrimSpace(t), payload)
	}
}

type BenchResult struct {
	Name       string
	WriteOps   float64
	ReadOps    float64
	AvgLatency float64
	P99Latency int64
}

func runBenchmark(name string, payload []byte) {
	var (
		setFn   func(ctx context.Context, key string, val []byte) error
		getFn   func(ctx context.Context, key string) ([]byte, error)
		cleanup func()
	)

	ctx := context.Background()

	switch name {
	case "slipstream":
		dir := *dataDir + "/slipstream"
		os.RemoveAll(dir)
		manager, err := wal.NewManager(dir)
		if err != nil {
			log.Printf("Slipstream init failed: %v", err)
			return
		}
		codec := func(b []byte) ([]byte, error) { return b, nil }
		decoder := func(b []byte) ([]byte, error) { return b, nil }
		db := engine.New[[]byte](manager, codec, decoder)

		setFn = func(ctx context.Context, k string, v []byte) error { return db.Put(ctx, k, v, 0) }
		getFn = func(ctx context.Context, k string) ([]byte, error) { return db.Get(ctx, k) }
		cleanup = func() { manager.Close() }

	case "bolt":
		dir := *dataDir + "/bolt"
		os.RemoveAll(dir)
		os.MkdirAll(dir, 0755)
		db, err := bolt.Open(dir+"/data.db", 0600, nil)
		if err != nil {
			log.Printf("Bolt init failed: %v", err)
			return
		}
		bucket := []byte("bench")
		db.Update(func(tx *bolt.Tx) error {
			_, _ = tx.CreateBucketIfNotExists(bucket)
			return nil
		})
		setFn = func(ctx context.Context, k string, v []byte) error {
			return db.Update(func(tx *bolt.Tx) error {
				return tx.Bucket(bucket).Put([]byte(k), v)
			})
		}
		getFn = func(ctx context.Context, k string) ([]byte, error) {
			var val []byte
			err := db.View(func(tx *bolt.Tx) error {
				val = tx.Bucket(bucket).Get([]byte(k))
				return nil
			})
			return val, err
		}
		cleanup = func() { db.Close() }

	case "badger":
		dir := *dataDir + "/badger"
		os.RemoveAll(dir)
		opts := badger.DefaultOptions(dir).WithLoggingLevel(badger.WARNING)
		db, err := badger.Open(opts)
		if err != nil {
			log.Printf("Badger init failed: %v", err)
			return
		}
		setFn = func(ctx context.Context, k string, v []byte) error {
			return db.Update(func(txn *badger.Txn) error {
				return txn.Set([]byte(k), v)
			})
		}
		getFn = func(ctx context.Context, k string) ([]byte, error) {
			var val []byte
			err := db.View(func(txn *badger.Txn) error {
				item, err := txn.Get([]byte(k))
				if err != nil {
					return err
				}
				val, err = item.ValueCopy(nil)
				return err
			})
			return val, err
		}
		cleanup = func() { db.Close() }

	case "redis":
		r := redis.NewClient(&redis.Options{Addr: *redisAddr})
		if err := r.Ping(ctx).Err(); err != nil {
			log.Printf("Redis connection failed: %v (skipping)", err)
			fmt.Printf("| %-15s | %-12s | %-12s | %-12s | %-12s |\n", name, "SKIP", "SKIP", "-", "-")
			return
		}
		setFn = func(ctx context.Context, k string, v []byte) error { return r.Set(ctx, k, v, 0).Err() }
		getFn = func(ctx context.Context, k string) ([]byte, error) { return r.Get(ctx, k).Bytes() }
		cleanup = func() { r.Close() }

	case "mongodb":
		client, err := mongo.Connect(ctx, options.Client().ApplyURI(*mongoAddr))
		if err != nil {
			log.Printf("MongoDB connection failed: %v (skipping)", err)
			fmt.Printf("| %-15s | %-12s | %-12s | %-12s | %-12s |\n", name, "SKIP", "SKIP", "-", "-")
			return
		}
		if err := client.Ping(ctx, nil); err != nil {
			log.Printf("MongoDB ping failed: %v (skipping)", err)
			fmt.Printf("| %-15s | %-12s | %-12s | %-12s | %-12s |\n", name, "SKIP", "SKIP", "-", "-")
			return
		}
		coll := client.Database("bench").Collection("kv")
		_, _ = coll.DeleteMany(ctx, bson.M{})

		setFn = func(ctx context.Context, k string, v []byte) error {
			_, err := coll.UpdateOne(ctx, bson.M{"_id": k}, bson.M{"$set": bson.M{"v": v}}, options.Update().SetUpsert(true))
			return err
		}
		getFn = func(ctx context.Context, k string) ([]byte, error) {
			var result struct{ V []byte }
			err := coll.FindOne(ctx, bson.M{"_id": k}).Decode(&result)
			return result.V, err
		}
		cleanup = func() { client.Disconnect(ctx) }

	default:
		log.Printf("Unknown target: %s", name)
		return
	}

	if cleanup != nil {
		defer cleanup()
	}

	writeOps, writeAvgLat, writeP99 := benchmarkOp(*requests, *concurrency, func(i int) {
		key := fmt.Sprintf("key:%d", i)
		_ = setFn(ctx, key, payload)
	})

	hotKeyCount := 100
	for i := range hotKeyCount {
		key := fmt.Sprintf("key:%d", i)
		_ = setFn(ctx, key, payload)
	}
	for i := range hotKeyCount {
		key := fmt.Sprintf("key:%d", i)
		_, _ = getFn(ctx, key)
	}
	readOps, readAvgLat, readP99 := benchmarkOp(*requests, *concurrency, func(i int) {
		key := fmt.Sprintf("key:%d", i%hotKeyCount)
		_, _ = getFn(ctx, key)
	})
	avgLat := (writeAvgLat + readAvgLat) / 2
	p99 := writeP99
	if readP99 > p99 {
		p99 = readP99
	}

	fmt.Printf("| %-15s | %-12.0f | %-12.0f | %-12.0f | %-12d |\n", name, writeOps, readOps, avgLat, p99)
}

func benchmarkOp(totalReqs, conc int, op func(i int)) (opsPerSec float64, avgLatNs float64, p99LatNs int64) {
	var wg sync.WaitGroup
	var ops int64
	var totalLatNs int64

	sampleRate := 100
	var latSamples []int64
	var latMu sync.Mutex

	chunk := totalReqs / conc

	start := time.Now()

	for w := 0; w < conc; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			localSamples := make([]int64, 0, chunk/sampleRate+1)
			base := workerID * chunk
			for j := range chunk {
				idx := base + j
				t0 := time.Now()
				op(idx)
				lat := time.Since(t0).Nanoseconds()
				atomic.AddInt64(&totalLatNs, lat)
				atomic.AddInt64(&ops, 1)

				if idx%sampleRate == 0 {
					localSamples = append(localSamples, lat)
				}
			}
			latMu.Lock()
			latSamples = append(latSamples, localSamples...)
			latMu.Unlock()
		}(w)
	}

	wg.Wait()
	elapsed := time.Since(start)

	if ops == 0 {
		return 0, 0, 0
	}

	opsPerSec = float64(ops) / elapsed.Seconds()
	avgLatNs = float64(totalLatNs) / float64(ops)

	// P99 from samples
	if len(latSamples) > 0 {
		sort.Slice(latSamples, func(i, j int) bool { return latSamples[i] < latSamples[j] })
		p99Idx := int(float64(len(latSamples)) * 0.99)
		if p99Idx >= len(latSamples) {
			p99Idx = len(latSamples) - 1
		}
		p99LatNs = latSamples[p99Idx]
	}

	return
}
