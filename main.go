package main

import (
	"flag"
	"fmt"
	"os"
)

func countElements() error {
	flag.Parse()
	if flag.NArg() != 1 {
		return fmt.Errorf("one filename expected")
	}
	path := flag.Arg(0)
	r, err := NewO5MReader(path)
	if err != nil {
		return err
	}

	nodes := 0
	ways := 0
	relations := 0
	for r.Next() {
		if r.Kind() == NodeKind {
			nodes += 1
		} else if r.Kind() == WayKind {
			ways += 1
		} else if r.Kind() == RelationKind {
			relations += 1
		}
	}
	if r.Err() != nil {
		return r.Err()
	}
	fmt.Println("nodes", nodes)
	fmt.Println("ways", ways)
	fmt.Println("relations", relations)
	return nil
}

func main() {
	err := countElements()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", err)
		os.Exit(1)
	}
}
