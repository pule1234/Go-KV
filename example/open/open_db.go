package main

import (
	"fmt"
	"path/filepath"
	"rose"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	// specify other options
	// opts.XXX
	opts := rose.DefaultOptions(path)
	db, err := rose.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}
	defer func() {
		_ = db.Close()
	}()
}
