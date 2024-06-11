package main

import (
	"fmt"
	"path/filepath"
	"rose"
)

func main() {
	path := filepath.Join("/tmp", "rosedb")
	opts := rose.DefaultOptions(path)
	db, err := rose.Open(opts)
	if err != nil {
		fmt.Printf("open rosedb err: %v", err)
		return
	}

	err = db.SAdd([]byte("fruits"), []byte("watermelon"), []byte("grape"), []byte("orange"), []byte("apple"))
	if err != nil {
		fmt.Printf("SAdd error: %v", err)
	}

	err = db.SAdd([]byte("fav-fruits"), []byte("orange"), []byte("melon"), []byte("strawberry"))
	if err != nil {
		fmt.Printf("SAdd error: %v", err)
	}

	diffSet, err := db.SDiff([]byte("fruits"), []byte("fav-fruits"))
	if err != nil {
		fmt.Printf("SDiff error: %v", err)
	}
	fmt.Println("SDiff set:")
	for _, val := range diffSet {
		fmt.Printf("%v\n", string(val))
	}

	unionSet, err := db.SUion([]byte("fruits"), []byte("fav-fruits"))
	if err != nil {
		fmt.Printf("SUnion error: %v", err)
	}
	fmt.Println("\nSUnion set:")
	for _, val := range unionSet {
		fmt.Printf("%v\n", string(val))
	}
}
