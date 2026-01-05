package main

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/mirkobrombin/go-cli-builder/v2/pkg/cli"
	"github.com/mirkobrombin/go-slipstream/pkg/engine"
	"github.com/mirkobrombin/go-slipstream/pkg/wal"
)

const DataDir = "./slipstream-data"

type DBWrapper struct {
	Engine  *engine.Engine[string]
	Manager *wal.Manager
}

func OpenDB() (*DBWrapper, error) {
	manager, err := wal.NewManager(DataDir)
	if err != nil {
		return nil, err
	}

	codec := func(s string) ([]byte, error) { return []byte(s), nil }
	decoder := func(b []byte) (string, error) { return string(b), nil }

	db := engine.New[string](manager, codec, decoder)
	if err := db.Recover(); err != nil {
		return nil, fmt.Errorf("recover failed: %w", err)
	}
	return &DBWrapper{Engine: db, Manager: manager}, nil
}

func (w *DBWrapper) Close() {
	w.Manager.Close()
}

// CLI Root
type CLI struct {
	Put     CmdPut     `cmd:"put" help:"Write a value"`
	Get     CmdGet     `cmd:"get" help:"Read a value"`
	Del     CmdDel     `cmd:"del" help:"Delete a value"`
	Load    CmdLoad    `cmd:"load" help:"Insert N random keys"`
	Compact CmdCompact `cmd:"compact" help:"Run compaction"`
	Stats   CmdStats   `cmd:"stats" help:"Show stats"`
	List    CmdList    `cmd:"list" help:"List keys"`
}

// Commands
type CmdPut struct {
	Key   string `arg:"" required:"true" help:"Key"`
	Value string `arg:"" required:"true" help:"Value"`
}

func (c *CmdPut) Run() error {
	db, err := OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()

	start := time.Now()
	if err := db.Engine.Put(context.Background(), c.Key, c.Value, 0); err != nil {
		return err
	}
	fmt.Printf("OK (%s)\n", time.Since(start))
	return nil
}

type CmdGet struct {
	Key string `arg:"" required:"true" help:"Key"`
}

func (c *CmdGet) Run() error {
	db, err := OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()

	start := time.Now()
	val, err := db.Engine.Get(context.Background(), c.Key)
	if err != nil {
		return fmt.Errorf("not found")
	}
	fmt.Printf("%s (%s)\n", val, time.Since(start))
	return nil
}

type CmdDel struct {
	Key string `arg:"" required:"true" help:"Key"`
}

func (c *CmdDel) Run() error {
	db, err := OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Engine.Delete(context.Background(), c.Key); err != nil {
		return err
	}
	fmt.Println("OK")
	return nil
}

type CmdLoad struct {
	Count int `arg:"" optional:"true" help:"Number of keys (default 1000)"`
}

func (c *CmdLoad) Run() error {
	count := c.Count
	if count <= 0 {
		count = 1000
	}

	db, err := OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()

	// Force smaller segments to see rotation
	db.Manager.SetMaxSegmentSize(1024 * 1024)

	fmt.Printf("Inserting %d keys...\n", count)
	start := time.Now()
	for i := 0; i < count; i++ {
		k := fmt.Sprintf("key-%d", rand.Intn(count*10))
		v := fmt.Sprintf("value-payload-%d", i)
		if err := db.Engine.Put(context.Background(), k, v, 0); err != nil {
			return err
		}
		if i%10000 == 0 && i > 0 {
			fmt.Printf("..%d\n", i)
		}
	}
	fmt.Printf("Done in %s\n", time.Since(start))
	return nil
}

type CmdCompact struct{}

func (c *CmdCompact) Run() error {
	db, err := OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()

	fmt.Println("Compacting...")
	start := time.Now()
	if err := db.Engine.Compact(); err != nil {
		return err
	}
	fmt.Printf("Done in %s\n", time.Since(start))
	return nil
}

type CmdStats struct{}

func (c *CmdStats) Run() error {
	db, err := OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()

	sealed := db.Manager.SealedSegments()
	fmt.Printf("Active Segment ID: %d\n", db.Manager.ActiveSegmentID())
	fmt.Printf("Sealed Segments: %d\n", len(sealed))
	for _, s := range sealed {
		fmt.Printf(" - ID: %d, Size: %d\n", s.ID(), s.Size())
	}
	return nil
}

type CmdList struct {
	Limit int `arg:"" optional:"true" help:"Max keys to list (default 10)"`
}

func (c *CmdList) Run() error {
	limit := c.Limit
	if limit <= 0 {
		limit = 10
	}

	db, err := OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()

	count := 0
	err = db.Engine.ForEach(func(key string, val string) error {
		if count >= limit {
			return fmt.Errorf("stop")
		}
		fmt.Printf("%s: %s\n", key, val)
		count++
		return nil
	})

	if err != nil && err.Error() != "stop" {
		return err
	}
	return nil
}

func main() {
	app := &CLI{}
	if err := cli.Run(app); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
